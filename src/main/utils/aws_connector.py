import sys
import boto3
import json
import botocore
from datetime import datetime
from botocore.exceptions import NoCredentialsError,PartialCredentialsError
from botocore.exceptions import ClientError


class AWSConnector():
    def __init__(self,logger,aws_section):
        self.logger = logger
        self.section = aws_section
        self.region = self.section['region']
        self.account_id = self.get_account_id()
        self.flag_setup_resource = 0
        self.kinesis_stream_start_time = datetime.now()
        self.retry_max_attempts = self.section['retry_max_attempts']
        self.retry_delay_seconds = self.section['retry_delay_seconds']
        self.cloudwatch_log_group = self.section['cloudwatch_log_group']
        self.cloudwatch_log_stream = self.section['cloudwatch_log_stream']
        self.session = boto3.Session()

    def get_sts_client(self):
        return boto3.client('sts')

    def get_logs_client(self):
        return boto3.client('logs')

    def get_cloudwatch_client(self):
        return boto3.client('cloudwatch')

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

    def waiter_log_retry(self,attempts,delay):
                self.logger.info(f'Waiter retrying ...Attemps {attempts}:delay {delay}seconds')
    
    def get_account_id(self):
        sts_client = self.get_sts_client()
        return sts_client.get_caller_identity().get('Account')
    
    def create_managed_policy(self,iam_client,policy,policy_document,policy_description):
        try:
            iam_client.create_policy(
            PolicyName=policy,
            PolicyDocument=policy_document,
            Description=policy_description
            )
            self.logger.info(f'Policy {policy} created')
            return True
        except iam_client.exceptions.EntityAlreadyExistsException:
            self.logger.info(f'Policy {policy} already exists')
            return True
        except Exception as e:
            self.logger.error(e,exc_info=True)
            return None

    def create_glue_table(self,glue_client,glue_database,glue_table):
        try:
            glue_client.create_database(DatabaseInput={'Name':glue_database})
        except glue_client.exceptions.AlreadyExistsException:
            self.logger.info(f'Glue database : {glue_database} already exists')
        except Exception as e:
            self.logger.error(e,exc_info=True)

        try:
            glue_client.create_table(
                DatabaseName=glue_database,
                TableInput={
                    'Name':glue_table,
                    'StorageDescriptor':{
                        'Columns':[
                            {'Name':'Data','Type':'struct<id:string,rank:string>'}
                        ]
                    }
                }
            )
            self.logger.info(f'Glue table : {glue_table} created in {self.region}')
            return True
        except glue_client.exceptions.AlreadyExistsException:
            self.logger.info(f'Glue table : {glue_table} already exists')
            return True
        except Exception as e:
            self.logger.error(f'Unable to create glue table {glue_table},{e}',exc_info=True)
            return None

    def create_kinesis_stream(self,kinesis_client,stream,shard_count,kinesis_delay,kinesis_max_attempts):
        try:
            self.kinesis_stream_start_time = datetime.now()
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
            self.logger.info(f'Kinesis stream :{stream} created in {self.region}')
            return True
        except kinesis_client.exceptions.ResourceInUseException:
            self.logger.info(f'Kinesis stream : {stream} already exists')
            return True
        except ClientError as e:
            self.logger.error(e,exc_info=True)
            raise

    def get_s3_bucket_ARN(self,bucket):
        try:
            s3_client = self.get_s3_client()
            s3_client.head_bucket(Bucket=bucket)
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
    
        
    def get_kinesis_firehose_ARN(self,stream):
        try:
            return  f'arn:aws:firehose:{self.region}:{self.account_id}:deliverystream/{stream}'
        except Exception as e:
            self.logger.error(e,exc_info=True)
            return None
    
    def get_glue_table_ARN(self,glue_database,glue_table):
        try:
            return f'arn:aws:glue:{self.region}:{self.account_id}:table/{glue_database}/{glue_table}'
        except Exception as e:
            self.logger.error(e,exc_info=True)
            return None

    def get_glue_database_ARN(self,glue_database):
        try:
            return f'arn:aws:glue:{self.region}:{self.account_id}:database/{glue_database}'
        except Exception as e:
            self.logger.error(e,exc_info=True)
            return None
    
    def get_glue_catalog_ARN(self):
        try:
            return f'arn:aws:glue:{self.region}:{self.account_id}:catalog'
        except Exception as e:
            self.logger.error(e,exc_info=True)
            return None
        
    def get_role_ARN(self,iam_client,role):
        try:
            return iam_client.get_role(RoleName=role)['Role']['Arn']
        except Exception as e:
            self.logger.error(e,exc_info=True)
            return None

    def get_role_policy_ARN(self,iam_client,role,policy):
        try:
            response_attached = iam_client.list_attached_role_policies(RoleName=role)
            role_policies = {policy['PolicyName']:policy['PolicyArn'] for policy in response_attached['AttachedPolicies']}
            if policy in role_policies:
                return role_policies.get(policy,None)
            else:
                return None
        except Exception as e:
            self.logger.error(e,exc_info=True)
            return None
    
    def get_unattached_policy_ARN(self,iam_client,role,policy,scope):
        try:
            response_unattached = iam_client.list_policies(Scope=scope,OnlyAttached=False)
            all_policies = {policy['PolicyName']:policy['Arn'] for policy in response_unattached['Policies']}
            if policy in all_policies:
                self.logger.info(f'policy ARN is: {all_policies.get(policy,None)} ')
                return all_policies.get(policy,None)
            else:
                return None
        except Exception as e:
            self.logger.error(e,exc_info=True)
            return None
           
    def get_service_policy_ARN(self,iam_client,service_role_policy):
        return f'arn:aws:iam::aws:policy/service-role/{service_role_policy}'
        
    def create_iam_role(self,iam_client,role,assume_policy_document):
        try:
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
        
    def attach_policy_to_role(self,iam_client,role,policy,policyARN):
        try:
            response = iam_client.attach_role_policy(
                        RoleName=role,
                        PolicyArn=policyARN
                    )
            self.logger.info(f'Policy {policy} attached to role {role}')
            return True
        except Exception as e:
            self.logger.error(f'Unable to attach policy:{policy} to {role}: {e}',exc_info=True)
            return None
        
        
    
    def verify_roles_policies(self,firehose_role,kinesis_stream_ARN,firehose_S3_bucket_ARN,glue_table_ARN,glue_database_ARN,firehose_ARN,catalog_ARN):
        try:
            dict_policy_iams = {
                'glue_table_ARN':glue_table_ARN,
                'glue_database_ARN':glue_database_ARN,
                'kinesis_stream_ARN':kinesis_stream_ARN,
                'firehose_S3_bucket_ARN':firehose_S3_bucket_ARN,
                'firehose_ARN': firehose_ARN,
                'catalog_ARN': catalog_ARN
            }
            iam_client = self.get_iam_client()
            assume_role_policy = json.loads(self.section['assume_role_policy'])
            self.create_iam_role(iam_client,firehose_role,assume_role_policy)
            managed_policies = json.dumps(json.loads(self.section['policies']))
            
            for policy,arn in dict_policy_iams.items():
                managed_policies = managed_policies.replace(policy,arn)
            managed_policies = json.loads(managed_policies)
            
            for policy in managed_policies:
                policy_name = policy.get('policy_name')
                policy_description = policy.get('policy_description')
                policy_document = json.dumps(policy.get('policy_document'))
                self.create_managed_policy(iam_client,policy_name,policy_document,policy_description)

                policy_ARN = self.get_role_policy_ARN(iam_client,firehose_role,policy_name)
                if not policy_ARN:
                        policy_ARN = self.get_unattached_policy_ARN(iam_client,firehose_role,policy_name,scope='Local')
                if policy_ARN:
                            response = self.attach_policy_to_role(iam_client,firehose_role,policy_name,policy_ARN)
                            if not response:
                                return False
            return True

        except Exception as e:
            self.logger.error(f'Unable to attach policies for {firehose_role}: {e}',exc_info=True)
            return None
    
    def create_cloudwatch_log_group(self,log_client):
        try:
            log_client.create_log_group(logGroupName=self.cloudwatch_log_group)
        except log_client.exceptions.ResourceAlreadyExistsException:
            self.logger.info(f'Log group : {self.cloudwatch_log_group} already exists')
            return True
        except Exception as e:
            self.logger.error(e,exc_info=True)
            raise
    
    def create_cloudwatch_log_stream(self,log_client):
        try:
            log_client.create_log_stream(logGroupName=self.cloudwatch_log_group,logStreamName=self.cloudwatch_log_stream)
        except log_client.exceptions.ResourceAlreadyExistsException:
            self.logger.info(f'Log stream : {self.cloudwatch_log_stream}  in group {self.cloudwatch_log_group} already exists')
            return True
        except Exception as e:
            self.logger.error(e,exc_info=True)
            raise

    def create_kinesis_firehose(self,firehose_client,firehose_stream,firehose_role,glue_database,glue_table,bucket_ARN,kinesis_stream_ARN):
        try:
            iam_client = self.get_iam_client()
            role_ARN = self.get_role_ARN(iam_client,firehose_role)

            firehose_client.describe_delivery_stream(DeliveryStreamName=firehose_stream)
            self.logger.info(f' Firehose stream {firehose_stream} found. Proceeding to next step')
            return True
        except firehose_client.exceptions.ResourceNotFoundException:
            self.logger.info(f'Firehose stream {firehose_stream} not found. Proceeding to create')
        except Exception as e:
            self.logger.error(e,exc_info=True)

        try:
            firehose_client.create_delivery_stream(
            DeliveryStreamName=firehose_stream,
            DeliveryStreamType='KinesisStreamAsSource',
            KinesisStreamSourceConfiguration={
                'KinesisStreamARN': kinesis_stream_ARN,
                'RoleARN': role_ARN
            },
            ExtendedS3DestinationConfiguration={
                'BucketARN': bucket_ARN,
                'RoleARN': role_ARN,
                'Prefix': self.stream_s3_prefix,
                'ErrorOutputPrefix':'error=!{firehose:error-output-type}/!{timestamp:yyyy/MM/dd}',
                'BufferingHints': {
                    'SizeInMBs': 64,
                    'IntervalInSeconds': 30
                },
                'CloudWatchLoggingOptions': {
                'Enabled': True,
                'LogGroupName': self.cloudwatch_log_group,
                'LogStreamName': self.cloudwatch_log_stream
                },
                'S3BackupMode': 'Disabled',
                'DataFormatConversionConfiguration': {
                        'SchemaConfiguration': {
                        'RoleARN':role_ARN,
                        'DatabaseName': glue_database,
                        'TableName': glue_table,
                        'Region': self.region,
                        'VersionId': 'LATEST'
                    },
                    'InputFormatConfiguration': {
                        'Deserializer': {
                            'OpenXJsonSerDe': {}
                        }
                    },
                    'OutputFormatConfiguration': {
                        'Serializer': {
                            'ParquetSerDe': {
                                'Compression':'GZIP'
                            }
                        }
                    },
                    'Enabled': True
                }
            }
        )
            self.logger.info(f'Firehose delivery stream {firehose_stream} created successfully.')
            return True
        except Exception as e:
            self.logger.error(e,exc_info=True)
            raise
    
    def check_for_data_persistance(self,cloudwatch_client,s3_client,firehose_stream):
        while True:
            firehose_metrics = cloudwatch_client.get_metric_statistics(
                Namespace='AWS/KinesisFirehose',
                MetricName='IncomingBytes',
                Dimensions=[
                {
                'Name': 'DeliveryStreamName',
                'Value': firehose_stream
                }
                ],
                StartTime=self.kinesis_stream_start_time,
                EndTime=datetime.now(),
                Period = 60,
                Statistics=['Sum'],
                Unit='Bytes'
            )
            #incoming_bytes = firehose_metrics['IncomingBytes']
            #s3_delivery = firehose_metrics['DeliveryToS3.Success']
            print(firehose_metrics)
            break
        return True
    
    def delete_kinesis_stream(self,kinesis_client,stream,kinesis_delay,kinesis_max_attempts):
        try:
            kinesis_client.describe_stream(StreamName=stream)
            self.logger.info(f'Kinesis stream:{stream} found in {self.region}. Proceeding to delete stream')
            kinesis_client.delete_stream(
                StreamName = stream
            )
            
            kinesis_waiter = kinesis_client.get_waiter('stream_not_exists')
            kinesis_waiter.config.max_attempts = kinesis_max_attempts
            kinesis_waiter.config.delay = kinesis_delay
            kinesis_waiter.config.retry_function= self.waiter_log_retry
            kinesis_waiter.wait(StreamName=stream)

            kinesis_client.describe_stream(StreamName=stream)
            self.logger.info(f'Kinesis stream:{stream} deleted in {self.region}')
            return True
        except kinesis_client.exceptions.ResourceNotFoundException:
            self.logger.info(f'Kinesis stream:{stream} deleted in {self.region}')
            return True
        except Exception as e:
            self.logger.error(e,exc_info=True)
            sys.exit(1)
    
    def delete_firehose_stream(self,firehose_client,stream,kinesis_delay,kinesis_max_attempts):
        try:
            firehose_client.describe_delivery_stream(DeliveryStreamName=stream)
            self.logger.info(f'firehose stream:{stream} found in {self.region}. Proceeding to delete stream')
            firehose_client.delete_delivery_stream(
                DeliveryStreamName = stream
            )
            
            firehose_client.describe_delivery_stream(DeliveryStreamName=stream)
            self.logger.info(f'Firehose stream:{stream} deleted in {self.region}')
            return True
        except firehose_client.exceptions.ResourceNotFoundException:
            self.logger.info(f'Firehose stream {stream} not found in {self.region}')
            return True
        except Exception as e:
            self.logger.error(e,exc_info=True)
            sys.exit(1)
    
    def setup_resources(self):
        try:
            kinesis_stream = self.section['kinesis_stream']
            firehose_stream = self.section['firehose_stream']
            firehose_role = self.section['firehose_role']
            
            glue_kinesis_database = self.section['glue_kinesis_database']
            glue_kinesis_table = self.section['glue_kinesis_table']
            firehose_s3_bucket = self.section['firehose_s3_bucket']
            firehose_s3_prefix = self.section['firehose_s3_prefix']
            self.stream_s3_prefix = firehose_s3_prefix+'/!{timestamp:yyyy/MM/dd}'
            shard_count = int(self.section['shard_count'])
            retry_delay=int(self.section['retry_delay_seconds'])
            retry_max_attempts=int(self.section['retry_max_attempts'])
            kinesis_client = self.get_kinesis_client()
            glue_client = self.get_glue_client()
            firehose_client = self.get_firehose_client()
            log_client = self.get_logs_client()

            self.logger.info(f'glue_kinesis_database:{glue_kinesis_database},glue_kinesis_table:{glue_kinesis_table}')

            self.create_glue_table(glue_client,glue_kinesis_database,glue_kinesis_table)
            kinesis_start_tiem = self.create_kinesis_stream(kinesis_client,kinesis_stream,shard_count,retry_delay,retry_max_attempts)

            kinesis_stream_ARN = self.get_kinesis_stream_ARN(kinesis_client,kinesis_stream)
            bucket_ARN = self.get_s3_bucket_ARN(firehose_s3_bucket)
            glue_table_ARN = self.get_glue_table_ARN(glue_kinesis_database,glue_kinesis_table)
            glue_database_ARN = self.get_glue_database_ARN(glue_kinesis_database)
            firehose_ARN = self.get_kinesis_firehose_ARN(firehose_stream)
            catalog_ARN = self.get_glue_catalog_ARN()
            self.create_cloudwatch_log_group(log_client)
            self.create_cloudwatch_log_stream(log_client)

            role_response = self.verify_roles_policies(firehose_role,kinesis_stream_ARN,bucket_ARN,glue_table_ARN,glue_database_ARN,firehose_ARN,catalog_ARN)
            
            if role_response:
                firehouse_reponse = self.create_kinesis_firehose(firehose_client,firehose_stream,firehose_role,glue_kinesis_database,glue_kinesis_table,bucket_ARN,kinesis_stream_ARN)
                if firehouse_reponse :
                    return True
                else:
                    self.logger.info(f'unable to create kinesis firehose {firehose_stream}')
            else:
                self.logger.error(f'Policies could not be attached to role {firehose_role}')
                sys.exit(1)
        except NoCredentialsError as e:
            self.logger.error(e,exc_info=True)
            sys.exit(1)
        except PartialCredentialsError as e :
            self.logger.error(e,exc_info=True)
            sys.exit(1)
        except Exception as e:
            self.logger.error(e,exc_info=True)
            sys.exit(1)

    def write_to_kinesis_stream(self,data):
        try:
            kinesis_stream = self.section['kinesis_stream']
            kinesis_client = self.get_kinesis_client()
            if not self.flag_setup_resource:
                self.setup_resources()
                self.flag_setup_resource = 1

            if self.flag_setup_resource:
                response = kinesis_client.put_records(
                            StreamName = kinesis_stream,
                            Records = data
                                )
                self.logger.info(f'Data written to stream {kinesis_stream} :{response}')
        except NoCredentialsError as e:
            self.logger.error(e,exc_info=True)
            sys.exit(1)
        except PartialCredentialsError as e :
            self.logger.error(e,exc_info=True)
            sys.exit(1)
        except Exception as e:
            self.logger.error(e,exc_info=True)
            sys.exit(1)
    
    def delete_streams(self):
        try:
            kinesis_client = self.get_kinesis_client()
            firehose_client = self.get_firehose_client()
            s3_client = self.get_s3_client()
            cloudwatch_client = self.get_cloudwatch_client()
            kinesis_stream = self.section['kinesis_stream']
            firehose_stream = self.section['firehose_stream']
            retry_delay=int(self.section['retry_delay_seconds'])
            retry_max_attempts=int(self.section['retry_max_attempts'])
            data_persisted = self.check_for_data_persistance(cloudwatch_client,s3_client,firehose_stream)
            if data_persisted :
                kinesis_reponse = self.delete_kinesis_stream(kinesis_client,kinesis_stream,retry_delay,retry_max_attempts)
                firehose_reponse = self.delete_firehose_stream(firehose_client,firehose_stream,retry_delay,retry_max_attempts)
                if kinesis_reponse and firehose_reponse:
                    self.logger.info(f'Deleted kinesis stream:{kinesis_stream} and firehose stream:{firehose_stream}')
        except Exception as e:
            self.logger.error(e,exc_info=True)

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
    
    def get_s3_bucket_prefix_count(self,bucket,prefix):
        pass
    
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
    
    def create_emr_cluster(self,emr_cluster_name):
        pass
    

        