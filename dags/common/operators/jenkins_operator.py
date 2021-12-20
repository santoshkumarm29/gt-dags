"""
Operator for remotely triggering Jenkins Builds
"""
import logging
import time

from airflow.models import BaseOperator
#from airflow.hooks import BaseHook
from airflow.hooks.base import BaseHook


from jenkinsapi.jenkins import Jenkins


class JenkinsOperator(BaseOperator):

    template_fields = ("job_name",)
    template_ext = (".sql",)
    ui_color = "#355564"

    def __init__(self, job_name=None, username="data-jenkins", conn_id="airflow_jenkins_conn", job_params={}, **kwargs):
        super().__init__(**kwargs)
        self.job_name = job_name
        self.username = username
        self.conn_id = conn_id
        self.job_params = job_params


    def execute(self, **kwargs):

        jenkins_token = BaseHook.get_connection(self.conn_id)
        token = jenkins_token.password

        server = Jenkins(
            "https://jenkins.datavertica.gsngames.com/jenkins/",
            username=self.username,
            password=token,
        )
        job = server.get_job(self.job_name)
        prev_id = job.get_last_buildnumber()
        logging.info(f'Building job: {self.job_name}, with params: {self.job_params}')
        server.build_job(self.job_name, params=self.job_params)
        while True:
            self.log.info("Waiting for build to start...")
            if prev_id != job.get_last_buildnumber():
                break
            time.sleep(3)
        self.log.info("Running...")
        last_build = job.get_last_build()
        while last_build.is_running():
            time.sleep(10)
            self.log.info("Running.....")
        self.log.info(str(last_build.get_status()))

        if str(last_build.get_status()) in (
            "FAIL",
            "FAILED",
            "FAILURE",
            "ERROR",
            "REGRESSION",
        ):
            raise Exception(
                f"Error Building tableau job Status : {last_build.get_status()}"
            )
        self.log.info("Job is complete!")
