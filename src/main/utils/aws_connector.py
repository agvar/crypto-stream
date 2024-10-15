import sys
import boto3
from botocore.exceptions import NoCredentialsError,PartialCredentialsError
from botocore.exceptions import ClientError
class AWSConnector():
    def __init__(self,logger,aws_section):
        self.logger = logger
        self.section = aws_section
        self.region = self.section['region']
        self.session = boto3.Session()

    def get_s3_client(self):
        return self.session.client('s3')

    def get_kinesis_client(self):
        return self.session.client('kinesis')
    
    def get_kinesis_stream(self,kinesis_client,stream,shard_count):
        try:
            kinesis_client.describe_stream(StreamName='cryptostream')
            self.logger.info(f'{stream} found. Proceeding to next step')
            return True
        except kinesis_client.exceptions.ResourceNotFoundException:
            self.logger.info(f'{stream} not found. Proceeding to create')
        except Exception as e:
            self.logger.error(e,exc_info=True)
            sys.exit(1)

        try:
            kinesis_client.create_stream(
                StreamName = stream,
                ShardCount = shard_count
            )
            self.logger.info(f'Kinesis stream : {stream} created in {self.region}')
            return True
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
        except PartialCredentialsError as e :
            self.logger.error(e,exc_info=True)
            return None
        except Exception as e:
            self.logger.error(e,exc_info=True)
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
        except PartialCredentialsError as e :
            self.logger.error(e,exc_info=True)
            return None
        except Exception as e:
            self.logger.error(e,exc_info=True)
            return None
        
    def write_to_kinesis_stream(self,data):
        try:
            kinesis_client = self.get_kinesis_client()
            self.logger.info(f'Created kinesis client :{kinesis_client}')
            stream = self.section['stream']
            shard_count = int(self.section['shard_count'])
            self.logger.info(f'stream:{stream},shard_count:{shard_count}')

            if self.get_kinesis_stream(kinesis_client,stream,shard_count):
                response = kinesis_client.put_records(
                                StreamName = stream,
                                Records = data
                            )
                self.logger.info(f'Data written to stream {stream} :{response}')
         
        except NoCredentialsError as e:
            self.logger.error(e,exc_info=True)
            return None
        except PartialCredentialsError as e :
            self.logger.error(e,exc_info=True)
            return None
        except Exception as e:
            self.logger.error(e,exc_info=True)
            return None 

    def read_from_kinesis_stream(self,stream,shard_id,iterator_type='LATEST'):
        try:
            kinesis_client = self.get_kinesis_client()
            stream = self.section['stream']
            iterator_type = self.section['iterator_type']

            response = kinesis_client.describe_stream(StreamName= stream,Region= self.region)
            shards = response['StreamDescription']['Shards']
            shard_ids = [shard['ShardId'] for shard in shards]
            for shard_id in shard_ids:
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
        except PartialCredentialsError as e :
            self.logger.error(e,exc_info=True)
            return None
        except Exception as e:
            self.logger.error(e,exc_info=True)
            return None

        