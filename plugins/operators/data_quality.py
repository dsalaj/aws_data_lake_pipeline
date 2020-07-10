from airflow.hooks.postgres_hook import PostgresHook
from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from helpers.sql_queries import SqlQueries

class DataQualityOperator(BaseOperator):

    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                 tables,
                 redshift_conn_id="redshift",
                 aws_credentials_id='aws_credentials',
                 *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        assert aws_credentials_id is not None
        self.redshift_conn_id = redshift_conn_id
        self.aws_credentials_id = aws_credentials_id
        self.tables = tables

    def execute(self, context):
        aws_hook = AwsHook(self.aws_credentials_id)
        credentials = aws_hook.get_credentials()
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        
        for table in self.tables:
            query = "SELECT COUNT(*) FROM {} WHERE {}id IS NULL".format(table, table[:-1])
            self.log.info("Testing quality with query:")
            self.log.info(query)
            results = redshift.get_first(query)
            assert results[0] == 0, "Number of rows with {} == NULL is {} (should be zero)".format(table[:-1], results[0])

