from airflow.models import BaseOperator
from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.exceptions import AirflowException
from airflow.utils.decorators import apply_defaults
from botocore.exceptions import ClientError
import time
import boto3

class RedshiftOperator(BaseOperator):
    template_fields = (
        'sql',
        'database',
        'work_group_name',
        'aws_conn_id',
        'region_name',
        )
    template_ext = (".sql",)

    @apply_defaults
    def __init__(
        self,
        sql,
        database,
        work_group_name,
        aws_conn_id='aws_default',
        region_name='ap-northeast-2',
        max_time=1200,
        sleep_time=30,
        **kwargs
    ):
        super().__init__(**kwargs)
        self.sql = sql
        self.database = database
        self.aws_conn_id = aws_conn_id
        self.work_group_name = work_group_name
        self.region_name = region_name
        self.sleep_time = sleep_time
        self.start_time = time.time()
        self.current_time = None
        self.max_time = max_time
        self.time_out = False
        self.query_execution_id = None


    def execute(self, context) -> str:
        aws_hook = AwsHook(self.aws_conn_id)
        creds = aws_hook.get_credentials()
        client = boto3.client(
            'redshift-data',
            aws_access_key_id=creds.access_key,
            aws_secret_access_key=creds.secret_key,
            region_name=self.region_name
        )

        query_object = client.execute_statement(
            Database=self.database,
            WorkgroupName=self.work_group_name,
            Sql=self.sql
        )

        self.query_statement_id = query_object['ResponseMetadata']['RequestId']

        while True:
            response = client.describe_statement(Id=self.query_statement_id)
            status = response['Status']
            self.current_time = time.time()

            if int(self.current_time - self.start_time) > self.max_time: # If the query execution time exceeds max time
                self.time_out = True
                break

            if status in ['FINISHED', 'FAILED', 'ABORTED', '']:
                break
            else:
                self.log.info("Query is still running. Sleep for %s seconds", self.sleep_time)
                time.sleep(self.sleep_time)

        if self.time_out:
            self.log.error("Query execution exceeds: %d seconds", self.max_time)
            raise AirflowException(f"Redshift query execution excceds {self.max_time} seconds")

        if status == 'FAILED':
            self.log.error("Query execution failed: %s", response['Error'])
            raise AirflowException("Redshift query run failed")
        elif status == 'ABORTED':
            self.log.warning("The query run was stopped by the user.")
            raise AirflowException("Redshift query run was stopped by the user")
        

        self.log.info("Redshift query has finished running")
        
        return self.query_execution_id
