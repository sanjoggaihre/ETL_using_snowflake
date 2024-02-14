from config import *
import snowflake.connector
import csv
import os
import pandas as pd


#Loading data into tables
def load_data(df, table_name, cur):
    #Removing existing data present in the table
    cur.execute(f"Truncate table {table_name}")

    for row in df.itertuples():
        sql_query = f'''
            Insert into {table_name} values {row[1:len(row)]}
        '''
        cur.execute(sql_query)


#Load table structure for stg and temp
def load_table(schema_name):
    
    conn = snowflake.connector.connect(
        user = SNOWFLAKE_USER,
        password = SNOWFLAKE_PASSWORD,
        account = SNOWFLAKE_ACCOUNT,
        warehouse = SNOWFLAKE_WAREHOUSE,
        database = SNOWFLAKE_DATABASE,
        schema = schema_name
    )
    cur = conn.cursor()
    for csv_file in os.listdir("Database_Records"):
        df = pd.read_csv(f"Database_Records/{csv_file}")
        df.fillna(value = 0, inplace=True)
        table_name = csv_file.split('.')[0]

        #Creating Tables in schema
        sql_query = f'''
            create or replace table {schema_name}_{table_name} as
            select * from TRANSACTIONS.{table_name} where 1=2;
        '''
        cur.execute(sql_query)

        load_data(df, f'{schema_name}_{table_name}', cur)
    print(f"{schema_name} TABLES LOADED SUCCESSFULLY")




