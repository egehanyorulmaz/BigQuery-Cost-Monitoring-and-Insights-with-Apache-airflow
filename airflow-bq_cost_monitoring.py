"""
A flow that extracts query details and billed prices/bytes
from Bigquery and inserts it to Greenplum. This pipeline can be configured using Airflow variables
in 3 different modes.\n
1. Daily\n\t
    Runs the pipeline for yesterday.\n
    Airflow Variable Requirements:\n\t
        * bq_cost_date_calculation_method = daily
2. Days Ago\n\t
    Runs the pipeline for the DAG trigger datetime and number of days ago specified as Airflow Variable.\n
    Airflow Variable Requirements:\n\t
        * bq_cost_date_calculation_method = days_ago\n\t
        * bq_cost_date_days_ago = NUMBER_OF_DAYS\n\t
            Example: 30, 60, 120 etc.\n
3. Days Between\n\t
    Runs the pipeline between the start and end date specified in Airflow variables.\n\t
    Airflow Variable Requirements:\n\t\t
        * bq_cost_date_calculation_method = days_between\n\t\t
        * bq_cost_job_start_date = START_DATETIME\n\t\t
            Example: 2022-07-15 00:00:00\n\t\t\t
        * bq_cost_job_end_date = END_DATETIME\n\t\t
            Example: 2022-07-20 08:00:00\n
Also, you can determine whether the target table will be truncated or not using Airflow Variable.\n
* bigquery_cost_truncate = BOOLEAN_EXPRESSION_AS_STRING\n\t
    Possible options: true, false\n\t
    Warning: Never use "true" if you specified the pipeline configuration as "daily"\n\t
    Default: "false"
"""
from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import PythonOperator
from airflow.operators.email import EmailOperator
from datetime import timedelta, datetime
import airflow
import os
from helpers.SqlQueryManager import BigQueryJobs

dag_name = 'BigQuery Cost Monitoring Pipeline'
dag_id = 'bigquery_cost_monitor'

default_args = {
    'owner': 'data',
    'depends_on_past': False,
    'retries': 1,
    'provide_context': False,
    'retry_delay': timedelta(minutes=5),
    'email': ['egehanyorulmaz@gmail.com'],
    'email_on_failure': True,
    'email_on_retry': True
}
CUR_DIR = os.path.abspath(os.path.dirname(__file__))

# days_ago, days_between, daily configurations
BQ_COST_DATE_CALCULATION_METHOD = airflow.models.Variable.get("bq_cost_date_calculation_method", "daily")
IS_PIPELINE_TRUNCATE_DESTINATION = airflow.models.Variable.get("bigquery_cost_truncate", "false")

if BQ_COST_DATE_CALCULATION_METHOD == 'daily':
    # runs the pipeline for yesterday
    yesterday_datetime = datetime.today() - timedelta(days=1)
    MAX_JOB_DATE = datetime.strptime(yesterday_datetime.strftime("%Y-%m-%d 23:59:59"), '%Y-%m-%d %H:%M:%S')
    MIN_JOB_DATE = datetime.strptime(yesterday_datetime.strftime("%Y-%m-%d 00:00:00"), '%Y-%m-%d %H:%M:%S')
elif BQ_COST_DATE_CALCULATION_METHOD == 'days_ago':
    days_ago = airflow.models.Variable.get("bq_cost_date_days_ago", 120)
    days_ago = int(days_ago)
    MAX_JOB_DATE = datetime.now()
    MIN_JOB_DATE = MAX_JOB_DATE - timedelta(days=days_ago)
elif BQ_COST_DATE_CALCULATION_METHOD == 'days_between':
    # Max job date
    airflow_max_job_date = airflow.models.Variable.get("bq_cost_job_end_date")
    MAX_JOB_DATE = datetime.strptime(airflow_max_job_date, '%Y-%m-%d %H:%M:%S')
    ## Min job date
    airflow_min_job_date = airflow.models.Variable.get("bq_cost_job_start_date")
    MIN_JOB_DATE = datetime.strptime(airflow_min_job_date, '%Y-%m-%d %H:%M:%S')


def extract_job_details():
    """
    Extracts job details for date interval configured using Airflow variables and
    saves the result as csv.
    :return:
    """
    bigquery_conn = BigQueryJobs()
    result_dict = bigquery_conn.get_all_job_details(min_creation_time=MIN_JOB_DATE, max_creation_time=MAX_JOB_DATE)
    result_dataframe = bigquery_conn.dict_to_tabular_format(result_dict)
    result_dataframe.to_csv(f"{CUR_DIR}/helpers/data/bq_data/bq_cost_tracker.csv", index=False)


with DAG(dag_id=dag_id,
         default_args=default_args,
         catchup=False,
         start_date=datetime(2022, 7, 28, 6, 0),
         schedule_interval="30 4 * * *",
         max_active_runs=1,
         concurrency=int(airflow.models.Variable.get('concurrency'))
         ) as dag:
    # Adds the doc at the beginning of python file to Airflow UI
    if hasattr(dag, 'doc_md'):
        dag.doc_md = __doc__

    dummy_start = DummyOperator(task_id="start")

    bq_execute_node = PythonOperator(task_id="bq_execute",
                                     python_callable=extract_job_details)

    send_email = EmailOperator(to="egehanyorulmaz@gmail.com",
                               cc="",
                               subject="Bigquery Cost Extraction Pipeline Successful",
                               task_id='send_success_email',
                               html_content=f"""<!DOCTYPE html>
                                                    <html lang="en">
                                                    <head>
                                                        <meta charset="UTF-8">
                                                        <title>INFO: Bigquery Cost Extraction Pipeline Successful</title>
                                                    </head>
                                                    <body>
                                                       <p> Hello,
                                                             <br></br>
                                                              <br> Daily GB and TRY costs are extracted from BigQuery successfully. </br>
                                                       </p>
            
                                                    </body>
                                                    </html>""")
    dummy_start >> bq_execute_node >> send_email