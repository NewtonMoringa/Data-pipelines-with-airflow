from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
import pandas as pd

default_args = {
    'owner': 'XYZ Telecoms',
    'depends_on_past': False,
    'start_date': datetime(2023, 3, 19),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

dag = DAG('data_pipeline', default_args=default_args, schedule_interval='@daily')

def extract_data():
    # extract data from CSV files
    # load the CSV data into Pandas dataframes for later transformation

def transform_data(): 
    # convert date fields to the correct format using pd.to_datetime
    # merge customer and order dataframes on the customer_id column
    # merge payment dataframe with the merged dataframe on the order_id and customer_id columns
    # drop unnecessary columns like customer_id and order_id
    # group the data by customer and aggregate the amount paid using sum
    # create a new column to calculate the total value of orders made by each customer
    # calculate the customer lifetime value using the formula CLV = (average order value) x (number of orders made per year) x (average customer lifespan) 

def load_data():
    # load the transformed data into Postgres database
    

with dag:
    
    # extract data 

    # transform data 

    # load data 

    # define dependencies extract_data >> transform_data >> load_data