import os
import psycopg2
import pandas as pd
from datetime import datetime, timedelta
from google.cloud import bigquery
import math
from typing import List, Dict
from sql_metadata import Parser

CUR_DIR = os.path.abspath(os.path.dirname(__file__))

class BigQuery():
    def __init__(self, ingestion_method='WRITE_TRUNCATE'):
        self.project_name = 'XXXXXX'
        self.credential_path = f"XXXXXX"
        self._client = self._get_bq_client()
        self._job_config = bigquery.LoadJobConfig(write_disposition=ingestion_method)

    def _get_bq_client(self) -> bigquery.Client:
        """
        Description: Saves credential path as environment variable and returns bigquery client.
        :return:
        """
        os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = self.credential_path
        client = bigquery.Client()
        return client

    @staticmethod
    def batch_generator(data_size, batchBigQueryJobs_size=50000):
        num_of_batches = math.ceil(data_size / batchBigQueryJobs_size)  # 11

        batch_combination = []
        for batch_no in range(num_of_batches):
            batch_start = batch_no * batchBigQueryJobs_size
            batch_end = (batch_no + 1) * batchBigQueryJobs_size - 1
            if batch_no == num_of_batches - 1:
                batch_end = data_size - 1
            batch_combination.append((batch_start, batch_end))
        return batch_combination

    def get_data_w_query(self, query_string):
        """
        Description: runs a query in BigQuery and return the result as dataframe.
        :param query_string: bigquery sql query
        :return:
        """
        dataframe = (
            self._client.query(query_string).result().to_dataframe(
                create_bqstorage_client=True))
        return dataframe

    def reformat_datetime_columns(self, data):
        """
        Description: Reformating for datetime and timestamp columns in bigquery
        :param data:
        :return:
        """
        for column_info in self._job_config.schema:
            if (column_info.field_type == 'DATETIME') | (column_info.field_type == 'TIMESTAMP'):
                column_name = column_info.name
                # DATETIME REFORMATING
                data[column_name] = data.created_at.apply(
                    lambda t: datetime.strptime(t.split('.')[0], '%Y-%m-%d %H:%M:%S'))
        return data

    def insert_to_bq(self, data, destination_schema, destination_table_name, project_name):
        table_id = f"{project_name}.{destination_schema}.{destination_table_name}"
        self._job_config.schema = self._client.get_table(table=table_id).schema
        print("Target table schema is as follows:\n ", self._job_config.schema)
        data = self.reformat_datetime_columns(data)
        tableref = self._client.dataset(dataset_id=destination_schema).table(destination_table_name)

        self._client.load_table_from_dataframe(data, tableref, job_config=self._job_config)
        print("Insertion to BigQuery is completed!")



class BigQueryJobs(BigQuery):
    def __init__(self):
        super().__init__()
        # for more information on bigquery analysis pricing for europe-west3 region
        # https://cloud.google.com/skus/?currency=USD&filter=bigquery+analysis
        # Note: The price mentioned in the link is for 1 tebibyte which is approximately 1.09 terabyte.
        # Price: 108.23 Turkish Lira -> 108.23/1.09 for TRY/Terabyte
        self.try_per_terabyte = 98.43

    def get_all_job_details(self, min_creation_time: datetime = None,
                            max_creation_time: datetime = None, max_result: int = 1000000):
        """
        Get and parse all jobs run under the project in Bigquery.
        :param min_creation_time:
        :param max_creation_time:
        :param max_result:
        :return:
        """
        global job_details

        # reduce default retry duration from 600 to 30
        retry = bigquery.DEFAULT_RETRY.with_deadline(30)

        # returns HTTPIterator that requests data from API everytime it is iterated
        jobs = self._client.list_jobs(project=self.project_name, max_results=max_result, all_users=True,
                                      min_creation_time=min_creation_time, max_creation_time=max_creation_time,
                                      retry=retry)
        collected_job_details = []
        for idx, job in enumerate(jobs):
            if idx % 100:
                print(idx)
            if type(job) == bigquery.job.load.LoadJob:
                job_details = {'job_type': 'load',
                               'created_at': job.created.strftime('%Y-%m-%d %H:%M:%S'),
                               'user_email': job.user_email,
                               'query': "",
                               'table_in_query': str(job.destination),
                               'run_time_seconds': (job.ended - job.started).seconds,
                               'gb_per_table': 0,
                               'try_per_table': 0}
                collected_job_details.append(job_details)

            elif type(job) == bigquery.job.query.QueryJob:
                query = job.query
                try:
                    tables_used_in_query = self.parse_query(query)
                    num_of_tables_in_query = len(tables_used_in_query)
                    for tables in tables_used_in_query:
                        job_details = {'job_type': 'query',
                                       'created_at': job.created.strftime('%Y-%m-%d %H:%M:%S'),
                                       'user_email': job.user_email,
                                       'query': query,
                                       'table_in_query': tables,
                                       'run_time_seconds': (job.ended - job.started).seconds,
                                       'gb_per_table': round(job.total_bytes_billed / (1e9 * (num_of_tables_in_query)),
                                                             2) if job.total_bytes_billed is not None else 0,
                                       'try_per_table': round((job.total_bytes_billed / 1e12) * self.try_per_terabyte,
                                                              2) if job.total_bytes_billed is not None else 0, }
                        collected_job_details.append(job_details)
                except ValueError as e:
                    print(e)

                except Exception as e:
                    print(query)
                    raise Exception(e)

        return collected_job_details

    def parse_query(self, query):
        """
        This method parses the query and extracts tables/views/materialized views used in the query.
        :param query: query string
        :return:
            list of tables/views/materialized views in the query
        """
        query_parser_obj = Parser(query)
        try:
            extracted_tables = query_parser_obj.tables
            return extracted_tables
        except IndexError as e:
            print(e)
            return ['']
        except AttributeError as e:
            # query with CTE expression may cause error
            print(e)
            extracted_tables = list(filter(lambda k: f'{self.project_name}' in k, query_parser_obj.tables))
            return extracted_tables
        except ValueError as e:
            # not supported query type
            print(e)
            extracted_tables = [Utils.find_between(query, '`', '`')]
            return extracted_tables

        except Exception as e:
            raise Exception(e)

    @staticmethod
    def dict_to_tabular_format(job_details: List[Dict]) -> pd.DataFrame:
        """
        This functions converts collected job details dictionary to tabular format for insertion to a relational database.
        :return:
        """
        columns = list(job_details[0].keys())
        maindf = pd.DataFrame(columns=columns)
        for job in job_details:
            tempdf = pd.DataFrame([job])
            maindf = pd.concat([maindf, tempdf])
        return maindf


class Utils:
    @staticmethod
    def find_between(s, first, last):
        """
        Find a string between first and last occurence of strings.
        :param s:
        :param first:
        :param last:
        :return:
        """
        try:
            start = s.index(first) + len(first)
            end = s.index(last, start)
            return s[start:end]
        except ValueError:
            return ""


# if __name__ == '__main__':
#     bigquery_conn = BigQueryJobs()
#     max_datetime = datetime.now()
#     min_datetime = max_datetime - timedelta(days=120)
#     result_dict = bigquery_conn.get_all_job_details(min_creation_time=min_datetime, max_creation_time=max_datetime)
#     result_dataframe = bigquery_conn.dict_to_tabular_format(result_dict)
