# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "63da7895-db51-4ecb-b43f-6761395b79ad",
# META       "default_lakehouse_name": "Bronze",
# META       "default_lakehouse_workspace_id": "64abe8bb-34b3-4d9a-b901-f47a9eb3359a",
# META       "known_lakehouses": [
# META         {
# META           "id": "1c20e8a7-f4bb-4bb1-aaf6-e0ad35ceb3cf"
# META         },
# META         {
# META           "id": "63da7895-db51-4ecb-b43f-6761395b79ad"
# META         }
# META       ]
# META     }
# META   }
# META }

# MARKDOWN ********************

# ## This notebook is the master notebook that converts CSV files from D365 F&O to Delta format. The files are exported from D365 F&O using Azure Synapse Link into Data lake folders. This notebook converts those files into Delta format.

# MARKDOWN ********************

# ### Parameters for the conversion

# PARAMETERS CELL ********************

processing_group = 'pg_daily'
table_names = ''
reload_data = True
#end_point = "m6hmogwbgd7ubovmhgwo3uydeu-xpukwzftgsne3oib6r5j5mzvti.database.fabric.microsoft.com"
end_point = "m6hmogwbgd7ubovmhgwo3uydeu-xpukwzftgsne3oib6r5j5mzvti.datawarehouse.fabric.microsoft.com"
port_number = "1433"
config_table = "dbConfiguration.config.tblConfiguration"

storage_account = "stgtwdemo002"
asl_container = "dataverse-twdemo002-unqba7b05a00b3af0119a43002248932"
base_path = 'Files/d365'
watermark_name = '_watermark'

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Accessing the SQL DB from the Notebook using JDBC driver. This is not required anymore with the availability of the spark warehouse connector

# CELL ********************

''' token = mssparkutils.credentials.getToken('pbi')
cs = 'm6hmogwbgd7ubovmhgwo3uydeu-5lo2ncan6rfuxdii6mnbmpdfwu.datawarehouse.fabric.microsoft.com'
jdbc_url = f'jdbc:sqlserver://{cs}:1433'
driver = 'com.microsoft.sqlserver.jdbc.SQLServerDriver'
csprop = {
    "accessToken": token,
    "driver": driver
}
db_name = 'dbConfiguration'

query = 'select * from dbConfiguration.config.tblConfiguration' 

df = spark.read.jdbc(url = f'{jdbc_url};databaseName={db_name}', table = f'({query}) temp', properties = csprop)

display(df)

'''

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Connect to the QL DB using spark warehouse connector

# CELL ********************

import com.microsoft.spark.fabric
from com.microsoft.spark.fabric.Constants import Constants

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

spark.conf.set("spark.datawarehouse.dbConfiguration.sqlendpoint", f"{end_point},{port_number}")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df = spark.read.synapsesql(f"{config_table}")
display(df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Iterate through the config and build the DAG

# CELL ********************

table_set = df.collect()
activities = []
for i, p in enumerate(table_set):
    activity = {
        "name": f"{i} - {p['table_name']}",
        "path": 'D365_CSV_Delta_Worker',
        "timeoutPerCellInSeconds": 3600,
        "args":
        {
            "storage_account_name": storage_account,
            "synapse_link_container": asl_container,
            "table_name": p['table_name'],
            "reload_table":reload_data,
            "base_path": base_path,
            "watermark_name": watermark_name
        }
    }
    activities.append(activity)
print(activities)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

DAG = {
    "activities": activities,
    "timeoutInSeconds": 3600
}

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

error_count=0
try:  
    output = mssparkutils.notebook.runMultiple(DAG) 
    return_code = 'PASS'  
    print(f'{return_code}:{output}\r\n')
except Exception as e:  
    output = f"{e.result}"  
    error_count+=1   
    return_code = 'FAIL'
    print(f'{return_code}:{output}\r\n') 

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
