from config import *
import snowflake.connector


##creating sales aggregation table in TMP schema
def sales_aggregation():
    conn = snowflake.connector.connect(
    user = SNOWFLAKE_USER,
    password = SNOWFLAKE_PASSWORD,
    account = SNOWFLAKE_ACCOUNT,
    warehouse = SNOWFLAKE_WAREHOUSE,
    database = SNOWFLAKE_DATABASE,
    schema = SNOWFLAKE_TMP_SCHEMA
    )

    cur = conn.cursor()

    query = """
    CREATE or REPLACE TABLE TMP_SALES_AGG AS
    (SELECT year, month , sum(amount) as monthly_sale
    from
    (select ID,quantity,amount, year(transaction_time) as year , monthname(transaction_time) as month from TMP_SALES)
    group by year, month
    order by year desc, month asc)
    """
    cur.execute(query)
    print("Sales aggregation table is created");

