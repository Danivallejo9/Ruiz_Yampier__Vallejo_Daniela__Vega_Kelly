from datetime import datetime
from airflow import DAG
from airflow.decorators import task
from airflow.operators.python import PythonOperator

from src.extract import extract
from src.load import load
from src.transform import transform_data


with DAG(
   dag_id="etl_shopping",
   start_date=datetime(2016, 1, 1),
   schedule="0 0 * * *",
   tags= ['ETL', 'CSV', 'sqlite'],
   ) as dag:
    
    extract = PythonOperator(
        task_id='extract_data',
        python_callable=extract,
        op_kwargs={
            'csv_folder': '/opt/airflow/dags/dataset/', 
            'csv_table_mapping': {
                'olist_customers_dataset.csv': 'customers',
                'olist_geolocation_dataset.csv': 'geolocation',
                'olist_order_items_dataset.csv': 'order_items',
                'olist_order_payments_dataset.csv': 'order_payments',
                'olist_order_reviews_dataset.csv': 'order_reviews',
                'olist_orders_dataset.csv': 'orders',
                'olist_products_dataset.csv': 'products',
                'olist_sellers_dataset.csv': 'sellers',
                'product_category_name_translation.csv': 'category_translation',
            }, 
            'public_holidays_url': 'https://date.nager.at/api/v3/PublicHolidays'
        }
    )
    load = PythonOperator(
        task_id='load_data',
        python_callable=load,
        op_kwargs={
            'database': 'sqlite:////opt/airflow/dags/olist.db', 
        },
        provide_context=True 
    )
    transform = PythonOperator(
        task_id='transform_data',
        python_callable=transform_data,
        op_kwargs={
            'database': 'sqlite:////opt/airflow/dags/olist.db',
            'output_folder': '/opt/airflow/dags/transformed_data/'
        }
    )



    extract >> load  >> transform 