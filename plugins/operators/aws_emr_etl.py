import boto3
from datetime import datetime
import os

from airflow.models import Variable
from airflow.exceptions import AirflowException
from airflow.models.baseoperator import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.contrib.hooks.aws_hook import AwsHook


class AWSEMROperator(BaseOperator):
    """
        A custom airflow operator that creates EMR -Clusters by using boto3 library with
        given configs from Airflow Variables.
    """

    ui_color = "#9CCBA2"

    @apply_defaults
    def __init__(self,
                 conn_id,
                 time_zone=None,
                 *args,
                 **kwargs):
        super(AWSEMROperator, self).__init__(*args, **kwargs)
        self.conn_id = conn_id
        self.time_zone = time_zone
        self.region = os.environ.get('AWS_DEFAULT_REGION')
        self.s3_bucket = os.environ.get('AWS_S3_BUCKET')

    def execute(self, context):
        """
            Creates a EMR on AWS.
        """
        self.log.info("Initialize AWS connection ...")
        aws_hook = AwsHook(self.conn_id)
        credentials = aws_hook.get_credentials()
        
        # get config variable based on cluster-type
        # config = Variable.get("aws_emr_cluster_config", default_var={}, deserialize_json=True)
        config = {
            "cluster_name": "udacity-cap-airflow-EMR-op",
            "release_label": "emr-5.29.0",
            "master_instance_type": "m4.xlarge",
            "slave_node_instance_type": "m4.2xlarge",
            "num_slave_nodes": 2,
            "ec2_key_name": "udacity-emr-key",
            "region": self.region,
            "bootstrap": {"name": "bootstrap_emr",
                          "path":  f"s3://{self.s3_bucket}/scripts/bootstrap_emr.sh"}
        }
        client = boto3.client("emr",
                              region_name=config["region"],
                              aws_access_key_id=credentials.access_key,
                              aws_secret_access_key=credentials.secret_key
                              )
        # create emr-cluster
        self.log.info(f"Creating EMR-Cluster ...")
        response = client.run_job_flow(
            Name=f"{config['cluster_name']}-airflow-{datetime.now(self.time_zone).strftime('%Y-%m-%d-%H-%M-%S')}",
            ReleaseLabel=config["release_label"],
            Applications=[
                {"Name": "Hadoop"},
                {"Name": "Spark"},
                {"Name": "Hive"},
                {"Name": "Livy"}
            ],
            Instances={
                "InstanceGroups": [
                    {
                        "Name": "Master nodes",
                        "Market": "ON_DEMAND",
                        "InstanceRole": "MASTER",
                        "InstanceType": config["master_instance_type"],
                        "InstanceCount": 1
                    },
                    {
                        "Name": "Slave nodes",
                        "Market": "ON_DEMAND",
                        "InstanceRole": "CORE",
                        "InstanceType": config["slave_node_instance_type"],
                        "InstanceCount": config["num_slave_nodes"]
                    }
                ],
                "Ec2KeyName": config["ec2_key_name"],
                "KeepJobFlowAliveWhenNoSteps": False,
                "TerminationProtected": False,
            },
            VisibleToAllUsers=True,
            JobFlowRole="EMR_EC2_DefaultRole",
            ServiceRole="EMR_DefaultRole",
            BootstrapActions=[
                {
                    "Name": config["bootstrap"]["name"],
                    "ScriptBootstrapAction": {
                        "Path": config["bootstrap"]["path"]
                    }
                },
            ],
            Configurations=[
                {
                    'Classification': "spark-env",
                    'Configurations': [
                        {
                            "Classification": "export",
                            "Properties": {
                                "PYSPARK_PYTHON": "/usr/bin/python3"
                            }
                        }
                    ]
                },
            ],
            Steps=[
                {
                    'Name': 'Run Spark ETL',
                    'ActionOnFailure': 'TERMINATE_CLUSTER',
                    'HadoopJarStep': {
                        'Jar': 'command-runner.jar',
                        'Args': ['spark-submit', '/home/hadoop/scripts/merge_data.py']
                    }
                }
            ],
        )

        if not response['ResponseMetadata']['HTTPStatusCode'] == 200:
            raise AirflowException(f"EMR-Cluster creation failed: {response}")

        else:
            self.log.info(f"Cluster {response['JobFlowId']} created with params: {response}")
            return response["JobFlowId"]

