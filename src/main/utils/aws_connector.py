import sys
import boto3
import json
from botocore.exceptions import NoCredentialsError,PartialCredentialsError
from botocore.exceptions import ClientError
class AWSConnector():
    def __init__(self,logger,aws_section):
        self.logger = logger
        self.section = aws_section
        self.region = self.section['region']
        self.retry_max_attempts = self.section['retry_max_attempts']
        self.retry_delay_seconds = self.section['retry_delay_seconds']
        self.session = boto3.Session()

    def get_s3_client(self):
        return self.session.client('s3')

    def get_kinesis_client(self):
        return self.session.client('kinesis')
    
    def get_firehose_client(self):
        return self.session.client('firehose')
    
    def get_iam_client(self):
        return self.session.client('iam')
    
    def get_glue_client(self):
        return self.session.client('glue')
    
    def get_s3_bucket_ARN(self,bucket):
        try:
            s3_client = self.get_s3_client()
            s3_client.meta.client.get_bucket_location(Bucket=bucket)
            return f'arn:aws:s3:::{bucket}'
        except s3_client.meta.client.exceptions_BUCKETNOTFOUND as e:
            self.logger.error(e,exc_info=True)
            return None

    
    def get_kinesis_stream_ARN(self,kinesis_client,stream):
        try:
            return kinesis_client.describe_stream(StreamName=stream)['StreamDescription']['StreamARN']
        except Exception as e:
            self.logger.error(e,exc_info=True)
            return None
    
    def waiter_log_retry(self,attempts,delay):
                self.logger.info(f'Waiter retrying ...Attemps {attempts}:delay {delay}seconds')
    
    def get_role_ARN(self,iam_client,role):
        try:
            return iam_client.get_role(RoleName=role)['Role']['Arn']
        except Exception as e:
            self.logger.error(e,exc_info=True)
            return None

    def get_policy_ARN(self,iam_client,role,policy):
        try:
            response_unattached = iam_client.list_policies(Scope='AWS',OnlyAttached=False)
            all_policies = {policy['PolicyName']:policy['Arn'] for policy in response_unattached['Policies']}
            response_attached = iam_client.list_attached_role_policies(RoleName=role)
            role_policies = {policy['PolicyName']:policy['PolicyArn'] for policy in response_attached['AttachedPolicies']}
            if policy in role_policies:
                return role_policies.get(policy,None)
            else:
                return all_policies.get(policy,None)
        except Exception as e:
            self.logger.error(e,exc_info=True)
            return None
    
    def get_service_policy_ARN(self,iam_client,service_role_policy):
        return f'arn:aws:iam::aws:policy/service-role/{service_role_policy}'
        
    def create_iam_role(self,iam_client,role,assume_policy_document):
        try:
            print(json.dumps(assume_policy_document))
            iam_client.create_role(
                RoleName=role,
                AssumeRolePolicyDocument=json.dumps(assume_policy_document)
            )
            self.logger.info(f'role {role} created')
            return True
        except iam_client.exceptions.EntityAlreadyExistsException:
            self.logger.info(f'role {role} already exists')
            return True
        except Exception as e:
            self.logger.error(f'Unable to create role for {role} : {e}',exc_info=True)
            return None
        
    def attach_policies_to_role(self,iam_client,role,policies):
        try:
            for policy in policies:
                policyARN = self.get_policy_ARN(iam_client,role,policy)
                if policyARN:
                    iam_client.attach_role_policy(
                        RoleName=role,
                        PolicyArn=policyARN
                    )
                    self.logger.info(f'Policy {policy} attached to role {role}')
                else:
                    self.logger.error(f'Unable to get policy ARN for policy: {policy}')
                    return False
            return True
        except Exception as e:
            self.logger.error(f'Unable to attach policies{policies} for {role}: {e}',exc_info=True)
            return None
    
    def get_glue_table(self,glue_client,glue_database,glue_table,glue_schema):
        try:
            glue_client.get_table(DatabaseName=glue_database,Name=glue_table)
        except glue_client.exceptions.EntityNotFoundException:
            self.logger.info(f'{glue_table} not found. Proceeding to create')
        except Exception as e:
            self.logger.error(e,exc_info=True)
            sys.exit(1)
            
        try:
            glue_client.create_database(DatabaseInput={'Name':glue_database})
            glue_client.create_table(
                DatabaseName=glue_database,
                TableInput={
                    'Name':'kinesis_stream_schema',
                    'StorageDescriptor':{
                        'Columns':[
                            {'Name':'Data','Type':'struct','fields':[
                                {'Name':'id','Type':'string'},
                                {'Name':'rank','Type':'string'},
                            ]},

                        ]
                    }
                }
            )
            glue_waiter = glue_client.get_waiter('table_exists')
            glue_waiter.config.max_attempts = self.retry_max_attempts
            glue_waiter.config.delay = self.retry_delay
            glue_waiter.config.retry_function= self.waiter_log_retry
            glue_waiter.wait(TableName=glue_table)

            glue_client.get_table(DatabaseName=glue_database, Name=glue_table)
            self.logger.info(f'Glue table : {glue_table} created in {self.region}')
            return True
        except Exception as e:
            self.logger.error("Unable to create glue table {glue_table}")
            return None

        
    def get_kinesis_firehose(self,firehose_stream,kinesis_stream,firehose_role,glue_database,glue_table,destinaton_bucket, destination_bucket_prefix):
        try:
            firehose_client = self.get_firehose_client()
            kinesis_client = self.get_kinesis_client()
            iam_client = self.get_iam_client()
            kinesis_stream_ARN = self.get_kinesis_stream_ARN(kinesis_client,kinesis_stream)
            role_ARN = self.get_role_ARN(iam_client,firehose_role)
            bucket_ARN = self.get_s3_bucket_ARN(destinaton_bucket)

            firehose_client.describe_delivery_stream(DeliveryStreamName=firehose_stream)
            self.logger.info(f' Firehose stream {firehose_stream} found. Proceeding to next step')
            return True
        except firehose_client.exceptions.ResourceNotFoundException:
            self.logger.info(f'Firehose stream {firehose_stream} not found. Proceeding to create')
        except Exception as e:
            self.logger.error(e,exc_info=True)
            sys.exit(1)

        try:
            firehose_client.create_delivery_stream(
            DeliveryStreamName=firehose_stream,
            KinesisStreamSourceConfiguration={
                'KinesisStreamARN': kinesis_stream_ARN,
                'RoleARN': role_ARN
            },
            ExtendedS3DestinationConfiguration={
                'BucketARN': bucket_ARN,
                'Prefix': destination_bucket_prefix,
                'BufferingHints': {
                    'SizeInMBs': 5,
                    'IntervalInSeconds': 300
                },
                'CompressionFormat': 'GZIP',
                'DataFormatConversionConfiguration': {
                    'Enabled': True,
                    'InputFormatConfiguration': {
                        'Deserializer': {
                            'OpenXJsonSerDe': {}
                        }
                    },
                    'OutputFormatConfiguration': {
                        'Serializer': {
                            'ParquetSerDe': {}
                        }
                    },
                    'SchemaConfiguration': {
                        'RoleARN':role_ARN,
                        'DatabaseName': glue_database,
                        'TableName': glue_table,
                        'Region': self.region,
                        'VersionId': 'LATEST'
                    }
                }
            }
        )
            print(f"Firehose delivery stream {firehose_stream} created successfully.")
        except Exception as e:
            print(f"Error creating Firehose delivery stream: {e}")
    
    def get_kinesis_stream(self,kinesis_client,stream,shard_count,kinesis_delay,kinesis_max_attempts):
        try:
            kinesis_client.describe_stream(StreamName=stream)
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
            
            kinesis_waiter = kinesis_client.get_waiter('stream_exists')
            kinesis_waiter.config.max_attempts = kinesis_max_attempts
            kinesis_waiter.config.delay = kinesis_delay
            kinesis_waiter.config.retry_function= self.waiter_log_retry
            kinesis_waiter.wait(StreamName=stream)

            kinesis_client.describe_stream(StreamName=stream)
            self.logger.info(f'Kinesis stream : {stream} created in {self.region}')
            return True
        except ClientError as e:
            self.logger.error(e,exc_info=True)
            return None
               
    def write_to_kinesis_stream(self,data):
        try:
            kinesis_client = self.get_kinesis_client()
            iam_client = self.get_iam_client()
            kinesis_stream = self.section['kinesis_stream']
            firehose_stream = self.section['firehose_stream']
            firehose_role = self.section['firehose_role']
            assume_role_policy = json.loads(self.section['assume_role_policy'])
            glue_kinesis_database = self.section['glue_kinesis_database']
            glue_kinesis_table = self.section['glue_kinesis_table']
            firehose_s3_bucket = self.section['firehose_s3_bucket']
            firehose_s3_prefix = self.section['firehose_s3_prefix']
            firehose_role_policies = self.section['firehose_role_policies'].split(',')
            shard_count = int(self.section['shard_count'])
            retry_delay=int(self.section['retry_delay_seconds'])
            retry_max_attempts=int(self.section['retry_max_attempts'])

            self.logger.info(f'stream:{kinesis_stream},shard_count:{shard_count}')

            kinesis_stream_response = self.get_kinesis_stream(kinesis_client,kinesis_stream,shard_count,retry_delay,retry_max_attempts)
            create_iam_role = self.create_iam_role(iam_client,firehose_role,assume_role_policy)
            if create_iam_role:
                self.logger.info(f'iam role:{firehose_role} ready')
                attach_policies_to_role = self.attach_policies_to_role(iam_client,firehose_role,firehose_role_policies)
                if attach_policies_to_role:
                    self.logger.info(f'polices :{firehose_role_policies} attached to role:{firehose_role}')

            firehouse_reponse = self.get_kinesis_firehose(firehose_stream,kinesis_stream,firehose_role,glue_kinesis_database,glue_kinesis_table,firehose_s3_bucket, firehose_s3_prefix)
            
            if firehouse_reponse and kinesis_stream_response:
                response = kinesis_client.put_records(
                                StreamName = kinesis_stream,
                                Records = data
                            )
                self.logger.info(f'Data written to stream {kinesis_stream} :{response}')
         
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
    

        