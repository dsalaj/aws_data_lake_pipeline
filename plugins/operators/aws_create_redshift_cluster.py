import boto3
from datetime import datetime
import os

from airflow.exceptions import AirflowException
from airflow.models.baseoperator import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.contrib.hooks.aws_hook import AwsHook


class AWSRedshiftOperator(BaseOperator):
    """
        Creates an AWS-Redshift cluster
    """

    ui_color = "#8DCBA2"

    @apply_defaults
    def __init__(self,
                 conn_id,
                 cluster_identifier,
                 time_zone=None,
                 *args,
                 **kwargs):
        super(AWSRedshiftOperator, self).__init__(*args, **kwargs)
        self.conn_id = conn_id
        self.time_zone = time_zone
        self.cluster_identifier = cluster_identifier

    def execute(self, context):
        self.log.info("Initialize AWS connection ...")
        aws_hook = AwsHook(self.conn_id)
        credentials = aws_hook.get_credentials()

        config = {
            "cluster_type": "multi-node",
            "node_type": "dc2.large",
            "num_nodes": 2,
            "region": os.environ.get('AWS_DEFAULT_REGION')
        }
        client = boto3.client("redshift",
                              region_name=config["region"],
                              aws_access_key_id=credentials.access_key,
                              aws_secret_access_key=credentials.secret_key
                              )

        # get custom redshift-db config from environment-variables
        db_name = os.environ["AWS_REDSHIFT_SCHEMA"]
        master_user = os.environ["AWS_REDSHIFT_USER"]
        master_pw = os.environ["AWS_REDSHIFT_PW"]
        # create emr-cluster
        self.log.info(f"Creating Redshift-Cluster ...")
        response = client.create_cluster(
            ClusterIdentifier=self.cluster_identifier,
            ClusterType=config["cluster_type"],
            NodeType=config["node_type"],
            NumberOfNodes=config["num_nodes"],
            DBName=db_name,
            MasterUsername=master_user,
            MasterUserPassword=master_pw
        )

        if not response['ResponseMetadata']['HTTPStatusCode'] == 200:
            raise AirflowException(f"Redshift-Cluster creation failed: {response}")
        else:
            self.log.info(f"Cluster {response['Cluster']['ClusterIdentifier']} created with params: {response}")
            return response["Cluster"]["ClusterIdentifier"]
