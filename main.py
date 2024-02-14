import extract
import stg_temp_loading
import trg_loading
import data_mart

extract.extract_csv_file()
stg_temp_loading.load_table("STG")
stg_temp_loading.load_table("TMP")
data_mart.sales_aggregation()
trg_loading.load_table_structure()



