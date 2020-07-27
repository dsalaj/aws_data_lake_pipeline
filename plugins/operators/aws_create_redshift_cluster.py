import os
import json
import boto3
from botocore.exceptions import ClientError
from datetime import datetime

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

        iam = boto3.client('iam',
                           region_name=config["region"],
                           aws_access_key_id=credentials.access_key,
                           aws_secret_access_key=credentials.secret_key
                           )
        redshift = boto3.client("redshift",
                                region_name=config["region"],
                                aws_access_key_id=credentials.access_key,
                                aws_secret_access_key=credentials.secret_key
                                )
        ec2 = boto3.resource('ec2',
                             region_name=config["region"],
                             aws_access_key_id=credentials.access_key,
                             aws_secret_access_key=credentials.secret_key
                             )

        # Create Role for accessing S3
        try:
            self.log.info("Creating a new IAM Role...")
            dwhRole = iam.create_role(
                Path='/',
                RoleName=os.environ.get('AWS_IAM_ROLE_NAME'),
                Description="Allows Redshift clusters to call AWS services",
                AssumeRolePolicyDocument=json.dumps(
                    {'Statement': [{'Action': 'sts:AssumeRole',
                                    'Effect': 'Allow',
                                    'Principal': {'Service': 'redshift.amazonaws.com'}}],
                     'Version': '2012-10-17'})
            )
        except iam.exceptions.EntityAlreadyExistsException as e:
            self.log.info(e)  # Continue if already exists

        self.log.info("Attaching Policy AmazonS3ReadOnlyAccess to IAM Role...")

        iam.attach_role_policy(RoleName=os.environ.get('AWS_IAM_ROLE_NAME'),
                               PolicyArn="arn:aws:iam::aws:policy/AmazonS3ReadOnlyAccess"
                               )['ResponseMetadata']['HTTPStatusCode']

        roleArn = iam.get_role(RoleName=os.environ.get('AWS_IAM_ROLE_NAME'))['Role']['Arn']
        self.log.info(roleArn)

        # get custom redshift-db config from environment-variables
        db_name = os.environ["AWS_REDSHIFT_SCHEMA"]
        master_user = os.environ["AWS_REDSHIFT_USER"]
        master_pw = os.environ["AWS_REDSHIFT_PW"]
        # create emr-cluster
        self.log.info(f"Creating Redshift-Cluster ...")
        response = redshift.create_cluster(
            ClusterIdentifier=self.cluster_identifier,
            ClusterType=config["cluster_type"],
            NodeType=config["node_type"],
            NumberOfNodes=config["num_nodes"],
            DBName=db_name,
            MasterUsername=master_user,
            MasterUserPassword=master_pw,
            IamRoles=[roleArn],
        )

        myClusterProps = redshift.describe_clusters(ClusterIdentifier=self.cluster_identifier)['Clusters'][0]

        self.log.info(f"Open an incoming TCP port to access the cluster ednpoint...")
        try:
            vpc = ec2.Vpc(id=myClusterProps['VpcId'])
            defaultSg = list(vpc.security_groups.all())[0]
            self.log.info(defaultSg)
            defaultSg.authorize_ingress(
                GroupName=defaultSg.group_name,
                CidrIp='0.0.0.0/0',
                IpProtocol='TCP',
                FromPort=int(5439),
                ToPort=int(5439)
            )
        except Exception as e:
            print(e)

        if not response['ResponseMetadata']['HTTPStatusCode'] == 200:
            raise AirflowException(f"Redshift-Cluster creation failed: {response}")
        else:
            self.log.info(f"Cluster {response['Cluster']['ClusterIdentifier']} created with params: {response}")
            return response["Cluster"]["ClusterIdentifier"]
