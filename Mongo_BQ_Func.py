from pymongo import MongoClient
import pandas as pd
import json
import ast
from pandas import json_normalize
from bson import json_util
import dateutil.parser
import configparser
from datetime import date, datetime
from google.cloud import bigquery
import google.auth
import os

def confReader():
    try:
        config = configparser.ConfigParser()
        config.read(os.path.dirname(__file__) + '/properties.conf')
        configs=config._sections
        return configs
    except Exception as e:
        print("confReader Error: "+str(e) )

conf=confReader()

bq_client = bigquery.Client()
    try:
        config = configparser.ConfigParser()
        config.read(os.path.dirname(__file__) + '/properties.conf')
        config.set(Section, Setting, str(dateutil.parser.parse(str(datetime.utcnow().isoformat()))))
        with open(os.path.dirname(__file__) + '/properties.conf', "w") as config_file:
            config.write(config_file)
    except Exception as e:
        print("confSet Error: "+str(e))            
               
def BQTable_Insert(data):
    try:
        print(data.head())

        print("BQTable_Insert() -- Started")

        print("BQTable_Insert() -- Setting Table ID")
        table_id = conf["BQ"]["tmp_table_id"]

        print("BQTable_Insert() -- Setting Job Configurtion")
        job_config = bigquery.LoadJobConfig(write_disposition="WRITE_TRUNCATE")

        print("BQTable_Insert() -- Submitting Insert Job API")
        job = bq_client.load_table_from_dataframe(data, table_id, job_config=job_config)  # Make an API request.
        job.result()  # Wait for the job to complete.

        print("BQTable_Insert() -- Formatting DML Query")
        InsertValue_string="("
        Update_string="UPDATE SET "
        lncols=len(list(data.columns))
        counter=1
        for col in list(data.columns):
            if counter < lncols:
                Update_string=str(Update_string)+str(col)+" = source."+str(col)+"\n, "
                InsertValue_string=str(InsertValue_string)+str(col)+", "
                counter+=1
            else:
                Update_string=str(Update_string)+str(col)+" = source."+str(col)+"\n"
                InsertValue_string=str(InsertValue_string)+str(col)+")"
                counter+=1
        print("BQTable_Insert() -- Declaring DML Query")
        dml_statement = f"""MERGE {conf["BQ"]["table_id"]} target 
            USING {conf["BQ"]["tmp_table_id"]} source
            ON target.id = source.id
            WHEN MATCHED THEN
                {Update_string}
            WHEN NOT MATCHED THEN
                INSERT {InsertValue_string}
                VALUES {InsertValue_string}
                """            
        print("BQTable_Insert() -- Submitting DML Merge Job")
        query_job = bq_client.query(dml_statement)  
        query_job.result()

        print("BQTable_Insert() -- Ended")
    except Exception as e:
        print("BQTable_Insert Error: "+str(e))    

def mongoDataExtract():
    try:
        print("mongoDataExtract() -- Started")

        print("mongoDataExtract() -- ENV VAR LAST_RUN Check")
        print(os.environ.get("LAST_RUN"))
        if os.environ.get("LAST_RUN"):
            print("mongoDataExtract() -- LAST_RUN Found")

            print("mongoDataExtract() -- Getting ENV VAR LAST_RUN Value")
            myDatetime=os.environ.get("LAST_RUN")

            print("mongoDataExtract() -- Updating LAST_RUN Value")
            os.environ["LAST_RUN"] = str(dateutil.parser.parse(str(datetime.utcnow().isoformat())))
            #confSet("DATE_META","iso_now")
        else:
            print("mongoDataExtract() -- LAST_RUN Not Found")

            print("mongoDataExtract() -- Getting Configured LAST_RUN Value")
            myDatetime=conf["DATE_META"]["iso_now"]

            print("mongoDataExtract() -- Updating LAST_RUN Value")
            os.environ["LAST_RUN"] = str(dateutil.parser.parse(str(datetime.utcnow().isoformat())))
        print(os.environ.get("LAST_RUN"))
        
        print("mongoDataExtract() -- Initializing MongoDB Client")    
        myclient = MongoClient(conf["MONGODB"]["conn_string"])

        print("mongoDataExtract() -- Connecting to Target Collection")
        mycol = myclient[conf["MONGODB"]["db"]][conf["MONGODB"]["col"]]
        
        print("mongoDataExtract() -- Parsing DateTime")
        myDatetime = dateutil.parser.parse(myDatetime)
        print("mongoDataExtract() -- Date is: "+str(myDatetime))

        print("mongoDataExtract() -- Setting Document Filter Query")
        query = {"modified" : { "$gt" : myDatetime}}

        print("mongoDataExtract() -- Submitting MongoDB Document Fetch Job")
        mydoc = mycol.find(query)

        ##----- Flatten JSON -----##
        print("mongoDataExtract() -- Flattening JSON Output")
        mydoc_json=json.dumps(list(mydoc), default=json_util.default)
        df = pd.json_normalize(json.loads(mydoc_json))

        print("mongoDataExtract() -- Ended")
        return df
    except Exception as e:
        print("mongoDataExtract Error: "+str(e))    

def dataPrep(df):
    try:    
        print("dataPrep() -- Started")
        ##----- Remove Columns for empty values -----##
        print("dataPrep() -- Remove Null Columns")  
        df = df.drop(df.loc[:, df.isnull().all()].columns.to_list(), axis=1)
        
        ##----- Clean-up Field Names -----##      
        print("dataPrep() -- Clean-up Field Names Section")         
        df.columns = df.columns.str.replace(r'$', '',regex = True)
        df.columns = df.columns.str.replace(r'.', '_',regex = True)
        df.columns = df.columns.str.replace(r'_id_oid', '_oid',regex = True)
        df.columns = df.columns.str.replace(r'_date_date', '_date',regex = True)

        ##----- Remove Columns with "updated_date" in column string -----##
        print("dataPrep() -- Remove Columns with updated_date in column string Section")
        df = df.drop(df.filter(regex='updated_date').columns, axis=1)
        df = df.drop(df.filter(regex='_updated').columns, axis=1)
        
        ##----- Datetime Fiedlds Type -----##
        print("dataPrep() -- Datetime Fields Type Section")
        df["created_date"] = pd.to_datetime(df.created_date)
        df["modified_date"] = pd.to_datetime(df.modified_date)
        df["start_date"] = pd.to_datetime(df.start_date)
        df["last_date"] = pd.to_datetime(df.last_date)
        df["end_date"] = pd.to_datetime(df.end_date)
        df["accept_date"] = pd.to_datetime(df.accept_date)
        
        ##----- INT Fiedlds Type -----##
        print("dataPrep() -- INTEGER Fields Type Section")
        
        print("dataPrep() -- FillNA with Zero")
        df['feedback_caller_call_rating'] = df['feedback_caller_call_rating'].fillna(0)
        df['feedback_caller_service_rating'] = df['feedback_caller_service_rating'].fillna(0)
        df['feedback_callee_call_rating'] = df['feedback_callee_call_rating'].fillna(0)
        df['charge_value'] = df['charge_value'].fillna(0)

        print("dataPrep() -- astype(int)")
        df["feedback_caller_call_rating"] = df["feedback_caller_call_rating"].astype(int)
        df["feedback_caller_service_rating"] = df["feedback_caller_service_rating"].astype(int)
        df["feedback_callee_call_rating"] = df["feedback_callee_call_rating"].astype(int)
        df["charge_value"] = df["charge_value"].astype(int)

        print("dataPrep() -- Ended")
        return df        
    except Exception as e:
        print("dataPrep Error: "+str(e))     
        
def MAIN(event,context):
    try:
        print("MAIN() -- Started")
        print("MAIN() -- Calling mongoDataExtract()")
        df=mongoDataExtract()
        print("MAIN() -- Data Extracted Empty Check")
        if df.empty:
            print("MAIN() -- No Data Captured, Exiting... .")
            return
        else:
            print("MAIN() -- Data Found")

            print("MAIN() -- Calling dataPrep()")
            df=dataPrep(df)
            print("MAIN() -- Calling BQTable_Insert()")
            BQTable_Insert(df)
        print("MAIN() -- Ended")
    except Exception as e:
        print("MAIN Error: "+str(e))    