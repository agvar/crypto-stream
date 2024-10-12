import boto3
import logging

# Enable logging
logging.basicConfig(level=logging.INFO)
logging.getLogger('botocore.credentials').setLevel(logging.DEBUG)
region='us-east-2'

session = boto3.Session()
print("AWS Credentials:")
print(f"Profile: {session.profile_name}")
print(f"Region: {session.region_name}")
print(f"Access Key: {session.get_credentials().access_key}")

# Example Boto3 client
s3_client = boto3.client('s3')
kinesis_client = boto3.client('kinesis')

# Try to list S3 buckets (this will trigger the credential search)
response = kinesis_client.describe_stream(StreamName='cryptostream')
print(response)