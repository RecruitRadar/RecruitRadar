from airflow.models import BaseOperator
from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.exceptions import AirflowException
from airflow.utils.decorators import apply_defaults
from typing import Sequence, Optional
from botocore.exceptions import ClientError
import time
import boto3

class RedshiftOperator(BaseOperator):

    """
    Executes SQL queries on Amazon Redshift using the 'redshift-data' boto3 client.

    Attributes:
        sql (str): SQL query to be executed.
        database (str): database: The name of the database to execute the query on.
        work_group_name (str): The name of the workgroup where the query will be executed.
        aws_conn_id (str): The Airflow connection ID to fetch AWS credentials.
        region_name (str): AWS region where Redshift is located.
        max_time (int): Maximum execution time for the query in seconds. Default is 1200 (20 minutes).
        sleep_time (int): Time interval to wait between polling for query status in seconds. Default is 30 seconds.
    """

    template_fields: Sequence[str] = (
        'sql',
        'database',
        'work_group_name',
        'aws_conn_id',
        'region_name',
        )
    template_ext: Sequence[str] = (".sql",)

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


    def execute(self, context) -> Optional[str]:
        """
        Execute the Redshift query.

        Args:
            context (dict): Airflow context parameters.

        Returns:
            Optional[str]: Query execution ID if successful, None otherwise.

        Raises:
            AirflowException: If the query execution failed or timed out.
        """
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

            if status in ['FINISHED', 'FAILED', 'ABORTED']:
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
