from airflow import DAG
from airflow.operators.neo4j_plugin import Neo4jOperator

from datetime import datetime, timedelta


default_args = {
    'owner': 'lucy',
    'depends_on_past': False,
    'start_date': datetime(2018, 11, 13),
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

dag = DAG('test_neo4j_dag', description='test neo4j connection', default_args=default_args,
          schedule_interval='0 12 * * *')

t1 = Neo4jOperator(task_id='neo4j_create',
                   cql="MERGE (node:AIRFLOW{name:'airflow1'}) RETURN node)",
                   dag=dag)
