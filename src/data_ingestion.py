import time
import requests
import json
import datetime
from datetime import timedelta
from pyspark.sql import SparkSession
import os


# noinspection PyTypeChecker
class DataIngest:
    def __init__(self, api_key: str):

        self.api_key = api_key
        self.job_queue = list()
        self.running_jobs = list()
        self.job_description = dict()
        


    @property
    def allow_additional_run(self):
        return False if len(self.running_jobs) >= 2 else True

    def get_data_by_range(self,
                          table: str,
                          start_date: datetime.datetime,
                          end_date: datetime.datetime = datetime.datetime.utcnow().date() - timedelta(days=1),
                          ):
        """
        Retrieves data from the given start_date to end_date (by default end_date is yesterday)
        """

        for year in range(start_date.year, end_date.year + 1):
            if year > start_date.year:
                start_month, start_day = 1, 1
            else:
                start_month, start_day = start_date.month, start_date.day

            if year < end_date.year:
                end_month, end_day = 12, 31
            else:
                end_month, end_day = end_date.month, end_date.day

            first_day = datetime.date(year, start_month, start_day).strftime("%m-%d-%Y")
            last_day = datetime.date(year, end_month, end_day).strftime("%m-%d-%Y")

            print(f"queuing request for {table} table between {first_day} and {last_day}")
            self.queue_job(first_day, last_day, table)

    def queue_job(self, start_date: str, end_date: str, table: str):
        """
        Creates a json payload and adds it to the list of jobs to be run
        """
        payload = {
            "start_date": start_date,
            "end_date": end_date,
            "api_key": self.api_key,
            "destination_s3_bucket": "allstar-training-mootech",
            "destination_s3_directory": f"raw_data/{table}/{start_date}-{end_date}",
            "table": table
        }

        self.job_queue.append(payload)

    def run_fetch(self):
        """
        Get the payload from the job_queue and send the request to the API endpoint
        """
        while len(self.job_queue) > 0 or len(self.running_jobs) > 0:
            if self.allow_additional_run and len(self.job_queue) > 0:
                payload = self.job_queue.pop(0)
                response = requests.post('https://en44bq5e33.execute-api.us-east-1.amazonaws.com/dev/fetch_data',
                                         data=json.dumps(payload))
                if response.status_code == 200:
                    job_id = response.json()['job_id']
                    self.job_description[
                        job_id] = f"Job to fetch {payload['table']} table between {payload['start_date']} and {payload['end_date']}"

                    print(f"{self.job_description[job_id]} STARTED")
                    self.running_jobs.append(job_id)
                else:

                    print(f"{self.job_description[job_id]} FAILED TO START")

            if len(self.running_jobs) > 0:
                self.monitor_jobs()

    def monitor_jobs(self):
        """
        monitors and logs the status of the running jobs at a fixed (20 second) interval
        """
        jobs_copy = self.running_jobs.copy()
        for job_id in jobs_copy:
            job_status = requests.get('https://en44bq5e33.execute-api.us-east-1.amazonaws.com/dev/job_status',
                                      data=json.dumps({'job_id': job_id})).json()['execution_status']

            print(f"{job_id}: {job_status}")
            if job_status == 'COMPLETE':

                print(f"{self.job_description[job_id]} COMPLETED")
                self.running_jobs.remove(job_id)
            time.sleep(5)
        time.sleep(20)
