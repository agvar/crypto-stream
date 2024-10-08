import boto3
from boto3 import NoCredentialsError,PartialCredentials
from botocore.exceptions import ClientError

class AWSConnector():
    def __init__(self,logger,aws_section):
        self.logger = logger
        self.section = aws_section
        self.region = self.section['region']
        self.session = boto3.Session(self.region)

    def get_s3_client(self):
        return self.session.client('s3')

    def get_kinesis_client(self):
        return self.session.client('kinesis')
    
    def get_kinesis_stream(self,kinesis_client,stream,shard_count,region):
        try:
            response = kinesis_client.describe_stream(StreamName=stream)
            return True
        except kinesis_client.exceptions.resourceNotFoundException:
            self.logger.info(f'{stream} not found. Proceeding to create')
        try:
            kinesis_client.create_stream(
                StreamName = stream,
                ShardCount = shard_count,
                Region = region
            )
        except ClientError as e:
            self.logger.error(e,exc_info=True)
            return None
    
    def read_from_s3(self,bucket,prefix,key):
        try:
            s3_client =  self.get_s3_client()
            object_key = f'{prefix}/{key}' if prefix else key
            response = s3_client.get_object(Bucket=bucket,Key=object_key )
            data = response['Body'].read().decode('utf-8')
            return data
        except s3_client.exceptions.NoSuchKey as e:
            self.logger.error(e,exc_info=True)
            return None
        except NoCredentialsError as e:
            self.logger.error(e,exc_info=True)
            return None
        except PartialCredentials as e :
            self.logger.error(e,exc_info=True)
            return None
        except Exception as e:
            self.logger(e,exc_info=True)
            return None
    
    def write_to_s3(self,bucket,prefix,key,data):
        try:
            s3_client =  self.get_s3_client()
            object_key = f'{prefix}/{key}' if prefix else key
            response = s3_client.put_object(Bucket=bucket,Key=object_key,Body=data)
            return response
        except NoCredentialsError as e:
            self.logger.error(e,exc_info=True)
            return None
        except PartialCredentials as e :
            self.logger.error(e,exc_info=True)
            return None
        except Exception as e:
            self.logger(e,exc_info=True)
            return None
        
    def write_to_kinesis_stream(self,data):
        try:
            kinesis_client = self.get_kinesis_client()
            stream = self.section['stream']
            shard_count = self.section['shard_count']
            partition_key = data[self.section['partition_key']]

            if self.get_kinesis_stream(kinesis_client,stream,shard_count,self.region):
                kinesis_client.put_record(
                    StreamName = stream,
                    Data = data if isinstance(data,bytes) else data.encode('utf-8'),
                    PartitionKey= partition_key
                )
            else:
                self.logger.error(f'Unable to create stream {stream}}',exc_info=True)
                return None
        except NoCredentialsError as e:
            self.logger.error(e,exc_info=True)
            return None
        except PartialCredentials as e :
            self.logger.error(e,exc_info=True)
            return None
        except Exception as e:
            self.logger(e,exc_info=True)
            return None 

    def read_from_kinesis_stream(self,stream,shard_id,iterator_type='LATEST'):
        try:
            kinesis_client = self.get_kinesis_client()
            shard_iterator_response = kinesis_client.get_shard_iterator(
                StreamName = stream,
                ShardId = shard_id,
                ShardIteratorType = iterator_type
            )

            shard_iterator = shard_iterator_response['ShardIterator']
            response = kinesis_client.get_records(ShardIterator=shard_iterator )
            records = response['Records']
            return records
        
        except NoCredentialsError as e:
            self.logger.error(e,exc_info=True)
            return None
        except PartialCredentials as e :
            self.logger.error(e,exc_info=True)
            return None
        except Exception as e:
            self.logger(e,exc_info=True)
            return None

        