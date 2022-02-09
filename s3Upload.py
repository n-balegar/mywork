import boto3
from boto3.s3.transfer import TransferConfig
import os
import threading
import sys

from botocore.exceptions import ClientError

bucket_name = 'usaa-first-aws-bucket-5'
s3_resource = boto3.resource('s3')
s3_bucket=boto3.client('s3')

def check_if_bucket_exists(bucket_name):
    print("Bucket Name is ",bucket_name)
    try:
        bucket_response=s3_bucket.head_bucket(Bucket=bucket_name)
        return True
    except ClientError as e:
        return False

def create_bucket(bucket_name):
    print("Bucket Name is ",bucket_name)
    try:
        s3_bucket.create_bucket(Bucket=bucket_name,CreateBucketConfiguration={
        'LocationConstraint': 'us-east-2'
    })
    except ClientError as e:
        print(e)
        return False
    return True

def create_key(bucket_name):
    s3_bucket.put_object(Bucket=bucket_name,Key='/movies/2022-02-05')

res=check_if_bucket_exists(bucket_name)


def initiate_multi_part(bucket_name):
    uploadId=s3_bucket.create_multipart_upload(Bucket=bucket_name,Key='/movies/2022-02-05')
    print(uploadId)
if not res:
    create_bucket(bucket_name)
    create_key(bucket_name)
    initiate_multi_part(bucket_name)
