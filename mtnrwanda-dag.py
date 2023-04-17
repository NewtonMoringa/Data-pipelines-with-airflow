from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
import pandas as pd

default_args = {
    'owner': 'MTN Telecoms',
    'depends_on_past': False,
    'start_date': datetime(2023, 4, 17),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

dag = DAG('data_pipeline', default_args=default_args, schedule_interval='@daily')

def extract_data():
    # extract data from CSV files
    # load the CSV data into Pandas dataframes for later transformation
    # Customer Data
    customer_data = pd.read_csv("customer_data.csv")
    # Order Data
    order_data = pd.read_csv("order_data.csv")
    # Payment Data
    payment_data = pd.read_csv("payment_data.csv")
    
    dfs = dict(customer_data=customer_data,order_data=order_data,payment_data=payment_data)
    return dfs

def transform_data(dfs):
    customer_data_df = dfs["customer_data.csv"]
    # Order Data
    order_data_df = dfs["order_data.csv"]
    # Payment Data
    payment_data_df = dfs["payment_data.csv"]
    # convert date fields to the correct format using pd.to_datetime
    customer_data_df['date_of_birth'] = pd.to_datetime(customer_data_df['date_of_birth'])
    order_data_df['order_date'] = pd.to_datetime(order_data_df['order_date'])
    payment_data_df['payment_date'] = pd.to_datetime(payment_data_df['payment_date'])
    # merge customer and order dataframes on the customer_id column
    customer_order_merged_df = pd.merge(customer_data_df, order_data_df, how="inner", on="customer_id")
    # merge payment dataframe with the merged dataframe on the order_id and customer_id columns
    all_merged_df = pd.merge(customer_order_merged_df, payment_data_df, how="inner", on=["customer_id", "order_id"])
    # drop unnecessary columns like customer_id and order_id
    all_merged_df = all_merged_df.drop(['order_id','payment_id','email','date_of_birth',], axis=1)
    # group the data by customer and aggregate the amount paid using sum
    all_merged_df.groupby("customer_id")
    # create a new column to calculate the total value of orders made by each customer
    all_merged_df['total_value'] = all_merged_df['amount']
    # calculate the customer lifetime value using the formula CLV = (average order value) x (number of orders made per year) x (average customer lifespan) 

def load_data(data):
    # Step 3: Connect to the target system and write the data
    #customer_id	first_name	last_name	country	gender	order_date	product	price	payment_date	amount	total_amount
    conn = psycopg2.connect(
        host="localhost",
        port=5432,
        database="mydatabase",
        user="myusername",
        password="mypassword"
    )

    cur = conn.cursor()
    cur.execute("CREATE TABLE IF NOT EXISTS customers_data (id SERIAL PRIMARY KEY, customer_id TEXT, first_name TEXT, last_name TEXT, country TEXT, gender TEXT, order_date TEXT, product TEXT, price TEXT, payment_date TEXT, amount TEXT, total_amount TEXT)")
    conn.commit()

    data.to_sql('customers_data', con=conn, if_exists='append', index=False)

    conn.close()

    # Step 4: Return a message indicating success
    return "Data loaded successfully"
    

with dag:
    
    # extract data 

    data = PythonOperator(task_id='extract_data', python_callable=extract_data)

    # transform data 
    dataframe = PythonOperator(task_id='transform_data', python_callable=transform_data(data))

    # load data 
    load  = PythonOperator(task_id='load_data', python_callable=load_data(dataframe)) 

    # define dependencies 
    data >> dataframe >> load
    