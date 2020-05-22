import datetime
from airflow import models
from airflow.operators.bash_operator import BashOperator
from airflow.operators.dummy_operator import DummyOperator

default_dag_args = {
    # https://airflow.apache.org/faq.html#what-s-the-deal-with-start-date
    'start_date': datetime.datetime(2020, 4, 29)
}

#define name of two new datasets
staging_dataset = 'kaggle2_workflow_staging'
modeled_dataset = 'kaggle2_workflow_modeled'

bq_query_start = 'bq query --use_legacy_sql=false '

#creates initial Health_Statistics table
create_health_sql = 'create or replace table ' + modeled_dataset + '''.Health_Statistics as
                      select *
                      from ''' + staging_dataset + '''.Health_Nutrition_Population_Statistics
                      WHERE metricCode = "SH.XPD.PCAP" or metricCode = "SH.XPD.TOTL.CD" 
                        or metricCode = "SH.STA.OW15.ZS" or metricCode = "SN.ITK.DEFC.ZS" 
                        or metricCode = "SP.DYN.TO65.FE.ZS" or metricCode = "SP.DYN.TO65.MA.ZS" '''

#creates initial Life_Statistics table
create_life_sql = 'create or replace table ' + modeled_dataset + '''.Life_Statistics as
                    select *
                    from ''' + staging_dataset + '''.Health_Nutrition_Population_Statistics
                    where metricCode = "SP.DYN.TO65.MA.ZS" or metricCode = "SP.DYN.TO65.FE.ZS" 
                    or metricCode = "SP.DYN.IMRT.IN" or metricCode= "SP.DYN.AMRT.MA" 
                    or metricCode = "SP.DYN.AMRT.FE" or metricCode= "SP.DYN.LE00.IN" 
                    or metricCode = "SP.DYN.LE00.MA.IN" or metricCode= "SP.DYN.LE00.FE.IN"
                    or metricCode = "SP.DYN.CDRT.IN"'''

#creates initial Population_Statistics table
create_pop_sql = 'create or replace table ' + modeled_dataset + '''.Population_Statistics as
                    select *
                    from ''' + staging_dataset + '''.Health_Nutrition_Population_Statistics
                    where metricCode = "SP.POP.TOTL" or metricCode = "SP.POP.TOTL.MA.ZS" 
                    or metricCode = "SP.POP.TOTL.FE.ZS" or metricCode = "SP.POP.GROW" 
                    or metricCode = "SP.DYN.TFRT.IN" or metricCode = "SP.DYN.CBRT.IN"'''

#creates initial Urban_Growth_Statistics table
create_urban_sql = 'create or replace table ' + modeled_dataset + '''.Urban_Growth_Statistics as
                      select * 
                      from ''' + staging_dataset + '.Health_Nutrition_Population_Statistics \
                      WHERE metricCode = "SP.URB.GROW" or metricCode = "SP.URB.TOTL.IN.ZS" or metricCode = "SP.URB.TOTL"'

with models.DAG(
        'kaggle2_workflow',
        schedule_interval=None,
        default_args=default_dag_args) as dag:

    #creates staging dataset
    create_staging = BashOperator(
            task_id='create_staging_dataset',
            bash_command='bq --location=US mk --dataset ' + staging_dataset)
    
    #creates modeled dataset
    create_modeled = BashOperator(
            task_id='create_modeled_dataset',
            bash_command='bq --location=US mk --dataset ' + modeled_dataset)
    
    #operator to load .CSV file into BigQuery and create staging dataset
    load_health_nutrition = BashOperator(
            task_id='load_health_nutrition',
            bash_command='bq --location=US load --skip_leading_rows=1 \
                         --source_format=CSV ' + staging_dataset + '.Health_Nutrition_Population_Statistics \
                         "gs://global_surface_temperatures/health_nutrition_population_dataset/HealthNutrition.csv" \
                          /home/jupyter/schema.json',
            trigger_rule='one_success')
   
    #allows for branching functionality
    branch = DummyOperator(
            task_id='branch',
            trigger_rule='all_done')

    create_health = BashOperator(
            task_id='create_health',
            bash_command=bq_query_start + "'" + create_health_sql + "'", 
            trigger_rule='one_success')
    
    create_life = BashOperator(
            task_id='create_life',
            bash_command=bq_query_start + "'" + create_life_sql + "'", 
            trigger_rule='one_success')
    
    create_pop = BashOperator(
            task_id='create_pop',
            bash_command=bq_query_start + "'" + create_pop_sql + "'", 
            trigger_rule='one_success')
        
    create_urban = BashOperator(
            task_id='create_urban',
            bash_command=bq_query_start + "'" + create_urban_sql + "'", 
            trigger_rule='one_success')
    
    health_direct = BashOperator(
            task_id='health_direct',
            bash_command='python /home/jupyter/airflow/dags/Health_Statistics_beam.py')
    
    life_direct = BashOperator(
            task_id='life_direct',
            bash_command='python /home/jupyter/airflow/dags/Life_Statistics_beam.py')
    
    pop_direct  = BashOperator(
            task_id='pop_direct',
            bash_command='python /home/jupyter/airflow/dags/Population_Statistics_beam.py')
    
    urban_direct = BashOperator(
            task_id='urban_direct',
            bash_command='python /home/jupyter/airflow/dags/Urban_Growth_Statistics_beam.py')
    
    health = BashOperator(
            task_id='health',
            bash_command='python /home/jupyter/airflow/dags/Health_Statistics_beam_dataflow.py')
    
    life = BashOperator(
            task_id='life',
            bash_command='python /home/jupyter/airflow/dags/Life_Statistics_beam_dataflow.py')
    
    pop  = BashOperator(
            task_id='pop',
            bash_command='python /home/jupyter/airflow/dags/Population_Statistics_beam_dataflow.py')
    
    urban = BashOperator(
            task_id='urban',
            bash_command='python /home/jupyter/airflow/dags/Urban_Growth_Statistics_beam_dataflow.py')
    
    create_staging >> create_modeled >> load_health_nutrition >> branch
    branch >> create_health >> health_direct >> health
    branch >> create_life >> life_direct >> life
    branch >> create_pop >> pop_direct >> pop
    branch >> create_urban >> urban_direct >> urban
    