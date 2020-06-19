import logging
import boto3
from botocore.exceptions import ClientError
'''
Abstractions over S3's operations with python.
This module provides high level abstractions for efficient
create bucket/upload files/download files/transfer files between buckets.

It handles several things for the user using python:
    * Create bucket
    * List the buckets
    * upload files
    * download files
    * transfer files between buckets
'''

class S3:

    def __init__(self, file_name, bucket, object_name=None, region=None):
        self.file_name = file_name
        self.bucket = bucket
        self.object_name = object_name
        self.region = region
        # If S3 object_name was not specified, use file_name
        if self.object_name is None:
            self.object_name = self.file_name

    def create_bucket(self):
        """Create an S3 bucket in a specified region

        If a region is not specified, the bucket is created in the S3 default
        region (us-east-1).

        :param bucket: Bucket to create
        :param region: String region to create bucket in, e.g., 'us-west-2'
        :return: True if bucket created, else False
        """

        # Create bucket
        try:
            if self.region is None:
                s3_client = boto3.client('s3')
                location = {'LocationConstraint': 'us-west-2'}
                s3_client.create_bucket(Bucket=self.bucket,
                                        CreateBucketConfiguration=location)
                # s3_client.create_bucket(Bucket=self.bucket)
            else:
                s3_client = boto3.client('s3', region_name=self.region)
                location = {'LocationConstraint': self.region}
                s3_client.create_bucket(Bucket=self.bucket,
                                        CreateBucketConfiguration=location)
            print('Bucket {} created!!'.format(self.bucket))
        except ClientError as e:
            logging.error(e)
            return False
        return True

    def list_buckets(self):
        # Retrieve the list of existing buckets
        s3 = boto3.client('s3')
        response = s3.list_buckets()

        # Output the bucket names
        print('Existing buckets:')
        for bucket in response['Buckets']:
            print(f'  {bucket["Name"]}')

    def upload_file(self):
        """Upload a file to an S3 bucket

        :param file_name: File to upload
        :param bucket: Bucket to upload to
        :param object_name: S3 object name. If not specified then file_name is used
        :return: True if file was uploaded, else False
        """

        # Upload the file
        s3_client = boto3.client('s3')
        try:
            response = s3_client.upload_file(self.file_name, self.bucket, self.object_name)
            print('file {f} uploaded in {b}'.format(f=self.file_name, b=self.bucket))
        except ClientError as e:
            logging.error(e)
            return False
        return True

    def download_file(self):
        s3 = boto3.client('s3')
        s3.download_file(self.bucket, self.object_name,self.file_name)

    @staticmethod
    def transfer_file(bucket_source, bucket_destination):
        s3 = boto3.resource('s3')
        clientname = boto3.client('s3')

        bucket = bucket_source
        try:
            response = clientname.list_objects(
                Bucket=bucket,
                MaxKeys=5
            )

            for record in response['Contents']:
                key = record['Key']
                copy_source = {'Bucket': bucket, 'Key': key}

                try:
                    dest_bucket = s3.Bucket(bucket_destination)
                    dest_bucket.copy(copy_source, key)
                    print('{} transfered to destination bucket'.format(key))
                except Exception as e:
                    print(e)
                    print('Error getting object {} from bucket {}. '.format(key, bucket))
                    raise e
        except Exception as e:
            print(e)
            raise e


if __name__ == '__main__':
    bucket = 'tst-demo-source1'
    # bucket = 'demo-destination'
    # bucket = 'ege-gcobi-lab-uw2'
    file_name = 'TestFile.txt'
    object_name = None
    region = None
    s3obj = S3(bucket=bucket, file_name=file_name, object_name=object_name, region=region)

    # create bucket
    s3obj.create_bucket()

    # list all buckets
    s3obj.list_buckets()

    # upload a file into bucket
    s3obj.upload_file()

    # download a file from s3 bucket
    s3obj.download_file()

    # trasfer a file to different bucket
    s3obj.transfer_file('tst-demo-source1', 'tst-demo-destination')