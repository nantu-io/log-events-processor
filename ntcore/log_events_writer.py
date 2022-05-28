import boto3
import sys

class LogEventsCloudWatchWriter:
    """
    A log event writer that ingests the logs data to aws cloudwatch.
    """
    def __init__(self, region, log_group): 
        self._client = boto3.client('logs', region_name=region)
        self._log_group = log_group
        self._sequence_tokens = dict()
        self._max_retries = 3

    def _create_log_stream(self, log_stream_name):
        self._client.create_log_stream(
            logGroupName = self._log_group,
            logStreamName = log_stream_name
        )

    def _put_log_event_without_retry(self, log_stream_name, log_event, sequence_token):
        """
        Puts log events to cloudwatch.
        """
        return self._client.put_log_events(
            logGroupName = self._log_group,
            logStreamName = log_stream_name,
            sequenceToken = sequence_token,
            logEvents = [
                {
                    'timestamp': log_event.timestamp,
                    'message': log_event.message
                }
            ],
        )

    def put_log_event(self, log_stream_name, log_event):
        """
        Puts log events to cloudwatch. This method retries the put operation with this pattern:

        * When the log stream does not exists, creates the logs stream in cloudwatch.
        * When the given sequence token is invalid, extract the valid sequence token from the error response.
        * When the sequense token is already used, retries with the next sequence token from the error response.
        """
        sequence_token = self._sequence_tokens[log_stream_name] if log_stream_name in self._sequence_tokens else "0"
        num_retries = 0
        while num_retries < self._max_retries:
            try:
                response = self._put_log_event_without_retry(log_stream_name, log_event, sequence_token)
                self._sequence_tokens[log_stream_name] = response['nextSequenceToken']
                break
            except self._client.exceptions.InvalidSequenceTokenException as e:
                sys.stderr.write('Invalid sequence token. Current retries: {0}'.format(num_retries))
                sequence_token = e.response['expectedSequenceToken']
                num_retries += 1
            except self._client.exceptions.ResourceNotFoundException:
                sys.stderr.write('Log stream not found. Current retries: {0}'.format(num_retries))
                self._create_log_stream(log_stream_name)
                sequence_token = "0"
                self._sequence_tokens[log_stream_name] = sequence_token
                num_retries += 1
            except self._client.exceptions.DataAlreadyAcceptedException as e:
                sys.stderr.write('Data already accepted. Current retries: {0}'.format(num_retries))
                sequence_token = e.response['expectedSequenceToken']
                num_retries += 1
            except Exception as e:
                sys.stderr.write(e)
                break