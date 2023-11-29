# The Snowpark package is required for Python Worksheets. 
# You can add more packages by selecting them using the Packages control and then importing them.

import snowflake.snowpark as snowpark
import numpy as np
import pandas as pd 
import functools
from snowflake.snowpark.functions import *
from snowflake.snowpark.types import *
import decimal as dec
from datetime import datetime
from snowflake.snowpark.window import *


def dq_framework(session: snowpark.Session) -> snowpark.DataFrame: 
    # Your code goes here, inside the "dq_framework" handler

    def numcheck(x,col_name):
        try:
            int(x) 
            return ""
        except:
            return "Column: {} Failed with value: {} is not an integer".format(col_name,x)
        
    def is_double(x,col_name):
        try:
            dec.Decimal(x)
            return ""
        except:
            return "Column: {} Failed with value: {} is not an double".format(col_name,x)
        
    def is_string(x,col_name):
        try:
            str(x)
            return ""
        except:
            return "Column: {} failed with value:{} is not a valid string".format(col_name,x)
        
    def is_datetime_format(x,col_name,timestamp_format):     
        try:
            datetime.strptime(x, timestamp_format)
            return ""
        except:
            return "Column: {} failed with parsing: {}  ".format(col_name, x)    
    
    
    type_check_dictionary={
        'INT':numcheck,
        'BIGINT':numcheck,
        'NUMERIC':numcheck,
        'SMALLINT':numcheck,
        'FLOAT':is_double,
        'DOUBLE':is_double,
        'DECIMAL':is_double,
        'STRING':is_string,
        'DATE':is_datetime_format
    }

    def get_type_check_rules(rules_df):
        
        rules_df_rows=rules_df.collect()
        
        type_check_dict={
        "columns":[],
        "partial_funs":[],
        "rule_col_alias":"type_error",
        "rule_row_alias_flag":"type_error_flag"}
        
        for row in rules_df_rows:
            # key for dictionary is col_type
            key_type=row['COL_TYPE'].upper().strip()
            # value is resultant of partial function 
            if key_type in ['DATE','TIMESTAMP']:
                #create partial function as required
                type_check_dict["columns"].append(row["COL_NAME"])
                type_check_dict["partial_funs"].append(timestamp_partial_func)
            else:
                #print("type_check_dictionary[key_type]: ",type_check_dictionary[key_type])
                #create partial function as required
                # append col_name, partial function result value to sub dictionary
                type_check_dict["columns"].append(row["COL_NAME"])
                type_check_dict["partial_funs"].append(partial_func)
            
        #print(type_check_dict)     
        return type_check_dict
    
    
    def get_rules(meta_data_df):
        #get_type_check_rules similarly we can add n no.of checks.
        rule_fetchers=[get_type_check_rules]
        rules=[]
        for rule_fetcher in rule_fetchers:
            rule=rule_fetcher(meta_data_df)
            if rule is not None:
                rules.append(rule)
        return rules
    

    # def dedup(src_db,src_tbl,dst_db,dst_tbl,prm_key,dup_db,dup_tbl):
        # df_stg3  = session.sql('select * from {}.{} where update_process_dt_id is null'.format(src_db,src_tbl))
        # lst_prm = prm_key.split(',')
        # window_spec = Window.partitionBy(lst_prm).orderBy(lst_prm)
        # df_stg3 = df_stg3.withColumn("row_number", row_number().over(window_spec))
        # 
        # df_stg3.filter("row_number = 1").drop("row_number","file_dt","file_nm").write.mode("append").format("delta").saveAsTable("{}.{}".format(dst_db,dst_tbl))
        # lst_rw= tuple(df_stg3.filter("row_number > 1").select("unique_row_id").rdd.flatMap(lambda x:x).collect())
        # try:
        #     session.sql('update {}.{} set row_status_col = "D" where unique_row_id in {}'.format(dup_db,dup_tbl,lst_rw))
        # except:
        #     pass
    

    #Start
    df = session.sql('select * from {}.{}.{}'.format('DS_POC','METADATA','TRANSACTIONS_BRONZE'))
    rules_df = session.sql('Select * from {}.{}.{}'.format('DS_POC','METADATA','RULES_CONFIG'))
    rules = get_rules(rules_df)

    rbc_debug_df,bad_data,good_data,error_df=apply_rbc_rules(df,rules)

  
    rbc_debug_df.show()
    bad_data.show()
    good_data.show()
    error_df.show()

    good_data.write.mode('overwrite').saveAsTable('DS_POC.METADATA.TRANSACTIONS_SILVER_1')
    rbc_debug_df.write.mode('overwrite').saveAsTable('DS_POC.METADATA.RBC_DEBUG_DF')
    bad_data.write.mode('overwrite').saveAsTable('DS_POC.METADATA.BAD_DATA')
    error_df.write.mode('overwrite').saveAsTable('DS_POC.METADATA.ERROR_DF')

    return 'Success'