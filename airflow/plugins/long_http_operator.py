from typing import Sequence
from airflow.providers.http.hooks.http import HttpHook
from airflow.providers.http.operators.http import SimpleHttpOperator
from airflow.exceptions import AirflowException
import requests

class InfiniteTimeoutHttpHook(HttpHook):
    def run(self, endpoint, data=None, headers=None, extra_options=None):
        with self.get_conn() as session:
            extra_options = extra_options or {}
            extra_options.setdefault('timeout', None)  # Here, we remove or set a very large timeout.
            url = self.base_url + endpoint  # Create the full URL by appending endpoint to the base URL
            self.log.info("Sending %s to %s with timeout %s", self.method, url, extra_options.get("timeout", "not set"))
            response = session.request(self.method, url, **extra_options)
            try:
                response.raise_for_status()
            except requests.exceptions.HTTPError as err:
                raise AirflowException(
                    f"HTTP error: {err.response.reason}, error code: {err.response.status_code}"
                )
            return response


class CustomSimpleHttpOperator(SimpleHttpOperator):
    '''
    Custom SimpleHttpOperator that allows for infinite timeout.
    '''
    # template_fields: Sequence[str] = ('http_conn_id','endpoint','method', 'data', 'headers', 'extra_options','response_check')
    
    def execute(self, context):
        http = InfiniteTimeoutHttpHook(self.method, http_conn_id=self.http_conn_id)

        self.log.info("Calling HTTP method")
        response = http.run(self.endpoint, self.data, self.headers, self.extra_options)

        if self.response_check:
            check_result = self.response_check(response)
            self.log.info(f"Response check result: {check_result}")

            return response.text
        
        if self.log_response:
            self.log.info(response.text)