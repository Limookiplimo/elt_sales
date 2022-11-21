import os
import time
from datetime import datetime
import pandas as pd
from airflow.decorators import task
from airflow.models.dag import DAG
from airflow.utils.task_group import TaskGroup
from airflow.hooks import MySqlHook
# from airflow.providers.mysql.hooks.mysql import MySqlHook
from airflow.hooks.base import BaseHook
from sqlalchemy import create_engine

# Define credentials
pwd = os.environ.get('PGPASS')
uid = os.environ.get('PGUID')
server = 'localhost'

success = {"table(s) processed": "Data imported successfuly"}


# Extract tasks
@task()
def get_tables():
    hook = MySqlHook(mysql_conn_id='localhost')
    sql = """ SELECT table_name FROM information_schema.tables WHERE table_schema = 'sales' """
    df = hook.get_pandas_df(sql)
    print(df)
    tbl_dict = df.to_dict('dict')
    return tbl_dict


# Load extracts
def load_data(tbl_dict: dict):
    conn = BaseHook.get_connection('postgres_default')
    engine = create_engine(
        f'postgresql://{conn.login}:{conn.password}@{conn.host}:{conn.port}/{conn.schema}')
    all_tbl_name = []
    start_time = time.time()

    # Access table elements
    for k, v in tbl_dict['table_name'].items():
        # print(v)
        all_tbl_name.append(v)
        rows_imported = 0
        sql = f'select * FROM {v}'
        hook = MySqlHook(mysql_conn_id='localhost')
        df = hook.get_pandas_df(sql)
        print(
            f'Importing rows {rows_imported} to {rows_imported + len(df)}... for table {v} ')

        # Save df to postgres
        df.to_sql(f"src_{v}", engine, if_exists='replace', index=False)
        rows_imported += len(df)
        # Indicate elapsed time
        print(
            f'Done. {str(round(time.time() - start_time, 2))} total seconds elapsed')
    print('Data imported successfully')
    return all_tbl_name


# Transformation tasks
# Transform customers table
@task()
def transform_srccustomers():
    conn = BaseHook.get_connection('postgres_default')
    engine = create_engine(
        f'postgresql://{conn.login}:{conn.password}@{conn.host}:{conn.port}/{conn.schema}')
    p_df = pd.read_sql_query('SELECT * FROM sales.customers', engine)
    # Drop some columns
    revised = p_df[['customer_code', 'custmer_name']]
    # Repalce nulls
    revised['customer_code'].fillna('0', inplace=True)
    revised['custmer_name'].fillna('NA', inplace=True)
    # Rename columns
    revised = revised.rename(
        columns={'customer_code': 'CustomerCode', 'custmer_name': 'CustomerName'})
    revised.to_sql(f'stg_customers', engine, if_exists='replace', index=False)
    return success


# Transform markets table
@task()
def transform_srcmarkets():
    conn = BaseHook.get_connection('postgres')
    engine = create_engine(
        f'postgresql://{conn.login}:{conn.password}@{conn.port}/{conn.schema}')
    p_df = pd.read_sql_query('SELECT * FROM sales.markets', engine)
    # Drop columns
    revised = p_df[['markets_code', 'markets_name']]
    # Rename columns
    revised = revised.rename(
        columns={'markets_code': 'MarketCode', 'markets_name': 'MarketName'})
    # Save to sql
    revised.to_sql(f'stg_markets', engine, if_exists='replace', index=False)
    return success


# Transform products table
@task()
def transform_srcproducts():
    conn = BaseHook.get_connection('postgres_default')
    engine = create_engine(
        f'postgresql://{conn.login}:{conn.password}@{conn.port}/{conn.schema}')
    p_df = pd.read_sql_query(f'SELECT * FROM sales.products', engine)

    # Rename columns
    revised = p_df.rename(
        columns={'product_code': 'ProductCode', 'product_type': 'ProductType'})

    # Save to sql
    revised.to_sql(f'stg_products', engine, if_exists='replace', index=False)
    return success


# Transform transactions table
@task()
def transform_srctransactions():
    conn = BaseHook.get_connection('postgres_default')
    engine = create_engine(
        f'postgresql://{conn.login}:{conn.password}@{conn.port}/{conn.schema}')
    p_df = pd.read_sql_query(f'SELECT * FROM sales.transactions', engine)

    # Rename columns
    revised = p_df.rename(columns={'product_code': 'ProductCode', 'customer_code': 'CustomerCode', 'market_code': 'MarketCode',
                          'order_date': 'Date', 'sales_qty': 'Quantity', 'sales_amount': 'Amount', 'currency': 'Currency'})

    # Save to sql
    revised.to_sql(f'stg_transactions', engine,
                   if_exists='replace', index=False)
    return success


# Transform date table
@task()
def transform_srcdate():
    conn = BaseHook.get_connection('postgres_default')
    engine = create_engine(
        f'postgresql://{conn.login}:{conn.password}@{conn.port}/{conn.schema}')
    p_df = pd.read_sql_query(f'SELECT * FROM sales.date', engine)

    # Drop columns
    revised = p_df[['date', 'year', 'month_name']]

    # Rename columns
    revised.rename(
        columns={'date': 'Date', 'year': 'Year', 'month_name': 'Month'})

    # Save to sql
    revised.to_sql(f'stg_date', engine, if_exists='replace', index=False)
    return success


# Load transforms
@task()
def prdsales_model():
    conn = BaseHook.get_connection('postgres_default')
    engine = create_engine(
        f'postgresql://{conn.login}:{conn.password}@{conn.port}/{conn.schema}')
    # Get staged table data
    st = pd.read_sql_query(f'SELECT * FROM sales.stg_transactions', engine)
    sc = pd.read_sql_query(f'SELECT * FROM sales.stg_customers', engine)
    sm = pd.read_sql_query(f'SELECT * FROM sales.stg_markets', engine)
    sp = pd.read_sql_query(f'SELECT * FROM sales.stg_products', engine)  
    sd = pd.read_sql_query(f'SELECT * FROM sales.stg_date', engine)

    # Join all tables
    merged = st.merge(sc, sm, sp, sd, how='right', on=[
                      'CustomerCode', 'MarketCode', 'ProductCode', 'Date'])
    # Save the merged table to sql
    merged.to_sql(f'prd_sales', engine, if_exists='replace', index=False)
    return success


# [START dag_task_group]
with DAG(dag_id='sales_etl_dag', schedule='0,9 *  * *', start_date=datetime(2022, 11, 17), catchup=False, tags=['sales_model']) as dag:

    # [How_to_task_group_section_1]
    with TaskGroup('extract_src_load', tooltip='Extract and load source data') as extract_src_load:
        src_sales_tbls = get_tables()
        load_sales = load_data(src_sales_tbls)

        # Define order
        src_sales_tbls >> load_sales

    # [How_to_task_group_section_2]
    with TaskGroup('transform_src_load', tooltipv='Transform and stage data') as transform_src_load:
        transform_srccustomers = transform_srccustomers()
        transform_srcmarkets = transform_srcmarkets()
        transform_srcproducts = transform_srcproducts()
        transform_srctransactions = transform_srctransactions()
        transform_srcdate = transform_srcdate()

        # Define order
        [transform_srccustomers, transform_srcmarkets, transform_srcproducts,
            transform_srctransactions, transform_srcdate]

    # [How_to_task_group_section_2]
    with TaskGroup('load_sales_model', tooltip='Final product model') as load_sales_model:
        prd_sales_model = prdsales_model()

        # Define order
        prd_sales_model

    # Task_group_order
    extract_src_load >> transform_src_load >> load_sales_model
