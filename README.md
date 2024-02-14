# ETL using snowflake
At first snowflake database is connected and then data present in one schema are extracted using 'extract.py'. Then we create another schemas called STG, TMP, and TRG and the extracted data are pushed 
into STG and TMP schema using STG_TABLE_NAME naming convention. Then we use created another TMP_sales_aggregation table using Sales table, which consists of the total monthly sales of the each year. 
and then we loaded all those data into TRG schema with sur_key as surroget key for each table in TRG schema and then loaded data in all columns except sur_key as this column is surrogate key and is considered
as a primary key for the each table in temp
![image](https://github.com/sanjoggaihre/ETL_using_snowflake/assets/43695490/1b8b734f-8ce7-4490-956c-6784e195b77e)
