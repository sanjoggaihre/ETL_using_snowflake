from config import *
import snowflake.connector
import csv
import os
import pandas as pd


#Loading data into tables
def load_data(df, table_name, cur):
    #Removing existing data present in the table
    cur.execute(f"Truncate table DWH_Sanjog_{table_name}")
    match table_name:
        case "CATEGORY":
            for row in df.itertuples():
                query = "Insert into DWH_SANJOG_CATEGORY" \
                "(ID, CATEGORY_DESC)"\
                "VALUES ('%s','%s')" % (row.ID, row.CATEGORY_DESC)
                cur.execute(query)

        case "COUNTRY":
            for row in df.itertuples():
                query = "Insert into DWH_SANJOG_COUNTRY" \
                "(ID, COUNTRY_DESC)"\
                "VALUES ('%s','%s')" % (row.ID, row.COUNTRY_DESC)
                cur.execute(query)

        case "CUSTOMER":
            for row in df.itertuples():
                query = "Insert into DWH_SANJOG_CUSTOMER" \
                "(ID, CUSTOMER_FIRST_NAME,CUSTOMER_MIDDLE_NAME,CUSTOMER_LAST_NAME,CUSTOMER_ADDRESS)"\
                "VALUES ('%s','%s','%s','%s','%s')" % (row.ID, row.CUSTOMER_FIRST_NAME, row.CUSTOMER_MIDDLE_NAME, row.CUSTOMER_LAST_NAME, row.CUSTOMER_ADDRESS)
                cur.execute(query)

        case "PRODUCT":
            for row in df.itertuples():
                query = "Insert into DWH_SANJOG_PRODUCT" \
                "(ID, SUBCATEGORY_ID,PRODUCT_DESC)"\
                "VALUES ('%s','%s','%s')" % (row.ID, row.SUBCATEGORY_ID, row.PRODUCT_DESC)
                cur.execute(query)

        case "REGION":
            for row in df.itertuples():
                query = "Insert into DWH_SANJOG_REGION" \
                "(ID, COUNTRY_ID,REGION_DESC)"\
                "VALUES ('%s','%s','%s')" % (row.ID, row.COUNTRY_ID, row.REGION_DESC)
                cur.execute(query)
                    
        case "SALES":
            for row in df.itertuples():
                query = "Insert into DWH_SANJOG_SALES" \
                "(ID, STORE_ID,PRODUCT_ID,CUSTOMER_ID,TRANSACTION_TIME,QUANTITY, AMOUNT, DISCOUNT)"\
                "VALUES ('%s','%s','%s','%s','%s','%s','%s','%s')" % (row.ID, row.STORE_ID, row.PRODUCT_ID, row.CUSTOMER_ID, row.TRANSACTION_TIME,row.QUANTITY,row.AMOUNT,row.DISCOUNT)
                cur.execute(query)

        case "STORE":
            for row in df.itertuples():
                query = "Insert into DWH_SANJOG_STORE" \
                "(ID, REGION_ID,STORE_DESC)"\
                "VALUES ('%s','%s','%s')" % (row.ID, row.REGION_ID, row.STORE_DESC)
                cur.execute(query)
        
        case "SUBCATEGORY":
            for row in df.itertuples():
                query = "Insert into DWH_SANJOG_SUBCATEGORY" \
                "(ID, CATEGORY_ID,SUBCATEGORY_DESC)"\
                "VALUES ('%s','%s','%s')" % (row.ID, row.CATEGORY_ID, row.SUBCATEGORY_DESC)
                cur.execute(query)

def load_table_structure():
    conn = snowflake.connector.connect(
    user = SNOWFLAKE_USER,
    password = SNOWFLAKE_PASSWORD,
    account = SNOWFLAKE_ACCOUNT,
    warehouse = SNOWFLAKE_WAREHOUSE,
    database = SNOWFLAKE_DATABASE,
    schema = SNOWFLAKE_TRG_SCHEMA
    )

    cur = conn.cursor()
    for csv_file in os.listdir("Database_Records"):
        df = pd.read_csv(f"Database_Records/{csv_file}")
        df.fillna(value = 0, inplace=True)
        table_name_suffix = csv_file.split('.')[0]

        #Creating Tables in TRG schema
        sql_query = f'''
            create or replace table DWH_Sanjog_{table_name_suffix} as
            select * from TRANSACTIONS.{table_name_suffix} where 1=2;
        '''
        cur.execute(sql_query)
        cur.execute(f"ALTER TABLE DWH_Sanjog_{table_name_suffix} ADD SUR_KEY integer UNIQUE NOT NULL PRIMARY KEY AUTOINCREMENT(1,1)")
        load_data(df, table_name_suffix, cur)
    print("TRG TABLES LOADED SUCCESSFULLY")
    cur.close()
    conn.close()




