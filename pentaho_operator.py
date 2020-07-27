# -*- coding: utf-8 -*-
"""
Created on Sat Jul 25 23:17:06 2020

@author: EDUTRA
"""



from airflow.exceptions import AirflowException
from airflow.hooks.http_hook import HttpHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
import time


class PentahoApiOperator(BaseOperator):
    """
    Calls Pentaho Api to start a wait for job execution
    :param pentaho_conn_id: The connection to run the operator against
    :type pentaho_conn_id: str
    :param job_name: The name of the job to run. (templated)
    :type job_name: str
    :param job_folder: The folder of the job to run. (templated)
    :type job_name: str
    :param timeout: Max time in seconds to wait for the job to complete. (templated)
    :type job_name: int
    :param cycle_interval: Time in secondos to check status. (templated)
    :type cycle_interval: int
    :param xcom_push: Push the response to Xcom (default: False).
        If xcom_push is True, response of an HTTP request will also
        be pushed to an XCom.
    :type xcom_push: bool
    :param log_response: Log the response (default: False)
    :type log_response: bool
    """

    template_fields = ['job_name', 'job_folder', 'timeout', 'cycle_interval']
    template_ext = ()
    ui_color = '#f4a460'

    @apply_defaults
    def __init__(self,
                 job_folder=None,
                 job_name=None,
                 timeout=0,
                 cycle_interval=5,
                 xcom_push=False,
                 pentaho_conn_id='pentaho_server_default',
                 log_response=False,
                 *args, **kwargs):
        super(PentahoApiOperator, self).__init__(*args, **kwargs)
        self.pentaho_conn_id = pentaho_conn_id
        self.method = 'GET'
        self.endpoint = '/pentaho/kettle'
        self.job_folder = job_folder
        self.job_name = job_name
        self.timeout = timeout
        self.cycle_interval = 5 if cycle_interval < 5 else cycle_interval
        self.headers = {}
        # self.response_check = response_check
        self.xcom_push_flag = xcom_push
        self.log_response = log_response

    def execute(self, context):
        http = HttpHook(self.method, http_conn_id=self.pentaho_conn_id)


        while True:

            status, response = self.get_job_status(http)

            if status != 'Running':
                break

            self.log.info(f'Job is alread runnig. Waiting {self.cycle_interval}s')
            time.sleep(self.cycle_interval)
        


        self.log.info("Calling HTTP method runJob")

        data = {"job": self.job_folder + self.job_name, "xml": "Y"}
        response = http.run(self.endpoint + '/runJob',
                            data,
                            {},
                            {})
        
        self.log.debug(response.text)

        if self.get_tag_value('result', response.text) == 'OK':
            
            while True:

                status, response = self.get_job_status(http)

                if status == 'Finished':
                    break

                if status == 'Finished (with errors)':
                    raise AirflowException(f"Job Finished with errors.\n{response.text}")

                time.sleep(self.cycle_interval)

        else:

            raise AirflowException(f"Could not start Job.\n{response.text}")

        if self.xcom_push_flag:
            return response.text

    def get_job_status(self, http):
        data = {"name": self.job_name, "xml": "Y"}
        response = http.run(self.endpoint + '/jobStatus',
                    data,
                    {},
                    {})

        self.log.debug(response.text)

        status = self.get_tag_value('status_desc', response.text)
        return status, response


    def get_tag_value(self, tag, response):
        start = response.find(f'<{tag}>')
        end = response.find(f'</{tag}>')
        return response[start+2+len(tag):end]
