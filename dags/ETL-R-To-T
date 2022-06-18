from airflow import DAG
from datetime import datetime, timedelta
from airflow.contrib.operators.bigquery_operator import BigQueryOperator
from airflow.contrib.operators.bigquery_check_operator import BigQueryCheckOperator

# -----------------------------------------------------------------------------------------
# first, create connection to big query
# Config variables
dag_config = Variable.get("bigquery_github_trends_variables", deserialize_json=True)
BQ_CONN_ID = dag_config["bq_conn_id"]
BQ_PROJECT = dag_config["dataLake"]
BQ_DATASET = dag_config["trustedZone"]
# -----------------------------------------------------------------------------------------
# inner functions
default_args = {
    'owner': 'Hassan',
    'retries': 5,
    'retry_delay': timedelta(minutes=5),
    'depends_on_past': True,
    'email_on_failure': True,
    'email_on_rety': True,
    'email': 'hasan.tavakoli1369@gmail.com'
}
dag = DAG(
    dag_id='Data_Engineering_Test',
    start_date=datetime(2022, 6, 1),
    schedule_interval='@@weekly',
    catchup=False,
    max_active_runs=1,
    default_args=default_args
)

insert_To_DimUser = BigQueryOperator(
    task_id='bq_write_to_factSteps',
    sql='''#standardSQL select _id,givenName,familyName,email from DataLake.rawDataZone.user''',
    write_disposition='WRITE_TRUNCATE',
    allow_large_results=True,
    use_legacy_sql=False
    , dag=dag
)

# ------------------------------------------------------------------------------------------------------------------------
# This Task will append to the table factSteps.
check_tablefactSteps = BigQueryCheckOperator(
    task_id='bq_check_factSteps',
    sql='''#standardSQLSELECT table_id FROM DataLake.rawDataZone.__TABLES_SUMMARY__ WHERE table_id = "{0}" '''.format(
        "{{ macros.ds_add(ds, -7) }}"),
    use_legacy_sql=False,
    dag=dag)

insert_to_factSteps = BigQueryOperator(
    task_id='bq_write_to_factSteps',
    sql=''' 
    #standardSQL select _userId__oid,count(*) as countSteps,"{{ yesterday_ds_nodash }}" 
    as createData" from DataLake.rawDataZone.steps 
    where FORMAT_TIMESTAMP("%Y%b%d",(TIMESTAMP_MICROS(createDateTime__date__numberLong))) BETWEEN  TIMESTAMP("{3}") AND TIMESTAMP("{4}") group by _userId__oid
     '''.format(
        "{{ macros.ds_add(ds, -7) }}", "{{ yesterday_ds_nodash}}")
    , destination_dataset_table='{0}.{1}.factSteps${2}'.format(
        BQ_PROJECT, BQ_DATASET, '{{ yesterday_ds_nodash }}'
    ),
    write_disposition='WRITE_APPEND',
    allow_large_results=True,
    use_legacy_sql=False,
    bigquery_conn_id=BQ_CONN_ID,
    dag=dag
)
# -----------------------------------------------------------------------------------------
# This job will append to the table factBloodpressure.
check_tablefactBloodpressure = BigQueryCheckOperator(
    task_id='bq_check_factBloodpressure',
    sql=''' #standardSQL SELECT table_id FROM `DataLake.rawDataZone.__TABLES_SUMMARY__` WHERE
    table_id = "{0}" '''.format(
        "{{ macros.ds_add(ds, -7) }}"
    ),
    use_legacy_sql=False,
    dag=dag
)

insert_to_factBloodpressure = BigQueryOperator(
    task_id='bq_write_to_factBloodpressure',
    sql='''#standardSQL select userId__oid,TIMESTAMP_MICROS(createDateTime__date__numberLong/1000) as  dataTime 
    ,systolicValue,diastolicValue 
    from DataLake.rawDataZone.bloodPressure where dataTime BETWEEN 
    TIMESTAMP("{3}") AND  TIMESTAMP("{4}")
    '''.format("{{ macros.ds_add(ds, -7) }}",
               "{{ yesterday_ds_nodash }}"
               )
    , destination_dataset_table='{0}.{1}.factBloodpressure${2}'.format(
        BQ_PROJECT, BQ_DATASET, '{{ yesterday_ds_nodash }}'
    ),
    write_disposition='WRITE_APPEND',
    allow_large_results=True,
    use_legacy_sql=False,
    dag=dag
)

insert_To_DimUser >> check_tablefactSteps >> insert_to_factSteps >> check_tablefactBloodpressure >> insert_to_factBloodpressure
