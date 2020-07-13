import boto3
import os

from airflow.exceptions import AirflowException
from airflow.models.baseoperator import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.contrib.hooks.aws_hook import AwsHook


class AWSS3UploadOperator(BaseOperator):
    """
        A custom airflow operator that uploads files to S3 bucket by using boto3 library with
        given configs from Airflow Variables.
    """

    ui_color = "#FF5566"

    @apply_defaults
    def __init__(self,
                 conn_id,
                 file_names,
                 time_zone=None,
                 *args,
                 **kwargs):
        super(AWSS3UploadOperator, self).__init__(*args, **kwargs)
        self.conn_id = conn_id
        self.time_zone = time_zone
        self.region = os.environ.get('AWS_DEFAULT_REGION')
        self.s3_bucket = os.environ.get('AWS_S3_BUCKET')
        self.file_names = file_names

    def execute(self, context):
        """
            Upload files to S3 on AWS.
        """
        self.log.info("S3 bucket path:")
        self.log.info(self.s3_bucket)
        self.log.info(type(self.s3_bucket))
        self.log.info([type(fn) for fn in self.file_names])
        self.log.info("Initialize AWS connection ...")
        aws_hook = AwsHook(self.conn_id)
        credentials = aws_hook.get_credentials()

        client = boto3.client("s3",
                              region_name=self.region,
                              aws_access_key_id=credentials.access_key,
                              aws_secret_access_key=credentials.secret_key
                              )

        self.log.info(f"Uploading files...")
        for file_name in self.file_names:
            client.upload_file(file_name, self.s3_bucket, file_name)
