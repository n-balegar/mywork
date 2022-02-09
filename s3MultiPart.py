import fileChunk
import boto3
from botocore.exceptions import ClientError


class s3Upload:
	
    def __init__(self, multiPartUploadDict={},s3BucketName=None,Key=None,multipart_limit_size_mib=512):  # old size = 4096
            self.file_parts = []
            self.s3_resource = boto3.resource('s3')
            self.s3_client = boto3.client('s3')
            self.multipart_limit_size = multipart_limit_size_mib * 1024 * 1024
            self.s3BucketName=s3BucketName
            self.Key=Key
            self.multiPartUploadDict={}
   
    def check_if_bucket_exists(self):
        print("########### inside check_if_bucket_exists Bucket Name is {self.s3BucketName} ###########")
        try:
            isBucketExists=self.s3_client.head_bucket(Bucket=self.s3BucketName)
            print(isBucketExists)
            return isBucketExists
        except ClientError as e:
            return False
    
    def create_buckets(self,s3BucketName):
        self.s3BucketName=s3BucketName
        print("########### inside create_buckets Bucket Name is {self.s3BucketName} ")
        try:
            isBucketExists=s3Upload.check_if_bucket_exists(self)
            if isBucketExists is False:
                print("########### Bucket {self.s3BucketName} doesn't exists, creating the bucket ###########")
                self.s3BucketName=self.s3_client.create_bucket(Bucket=self.s3BucketName,CreateBucketConfiguration={
            'LocationConstraint': 'us-east-2'
        }) 
                return True
            else:
                print("Bucket Exists already in s3",self.s3BucketName)
                return False
        except ClientError as e:
            print(e)
            return False
    
   
	
    def initiateMultiPartUpload(self):
        multiPartUploadDict=self.s3_client.create_multipart_upload(Bucket=self.s3BucketName,Key=self.Key)
        print(multiPartUploadDict)
     
    
    def multipart_upload():
        pass
        

s3=s3Upload(Key='ratings',multipart_limit_size_mib=100)
response=s3.create_buckets('usaa-first-aws-bucket-6')
print("outside class ",response)
s3.initiateMultiPartUpload()
#print("outside class 1",s3.multiPartUploadDict.items())

#origin_path='/mnt/d/movieLense/raw/rating.csv'
#tmp_dir='/mnt/d/movieLense/chunk/'

#callChunk(origin_path,tmp_dir)
