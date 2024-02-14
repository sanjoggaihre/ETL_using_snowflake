from config import *
import snowflake.connector
import csv
import os

#snowflake connection
conn = snowflake.connector.connect(
    user = SNOWFLAKE_USER,
    password = SNOWFLAKE_PASSWORD,
    account = SNOWFLAKE_ACCOUNT,
    warehouse = SNOWFLAKE_WAREHOUSE,
    database = SNOWFLAKE_DATABASE,
    schema = SNOWFLAKE_SCHEMA
)
output_dir = 'D:/Bootcamp/ETL_Assignment/Database_Records'
cur = conn.cursor()
all_tables = []

def extract_csv_file():
    #storing all tables names
    for row in cur.execute("SHOW TABLES").fetchall():
        all_tables.append(row[1])

    for table in all_tables:
    #extracting columns_name of table
        columns_name = cur.execute(f"DESCRIBE TABLE {table}").fetchall()
        extracted_columns = [item[0] for item in columns_name]

        sql_query = f'''SELECT * FROM {table}'''
            
        #extract all row of a table in rows variable
        rows = cur.execute(sql_query).fetchall()
        
        file = open(os.path.join(output_dir, f'{table}.csv'), 'w+', newline ='')
        with file:    
            write = csv.writer(file)
            write.writerow(extracted_columns)
            write.writerows(rows)
