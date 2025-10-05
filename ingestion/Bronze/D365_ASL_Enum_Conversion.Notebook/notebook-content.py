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

# ### Parameters

# PARAMETERS CELL ********************

storage_account = "stgtwdemo002"
asl_container = "dataverse-twdemo002-unqba7b05a00b3af0119a43002248932"
base_path = 'Files/d365'

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Notebook parameters

# CELL ********************

table_name = '_enum_translations'
delta_base_path = f'{base_path}/delta'
csv_base_path = f'{base_path}/csv'
delta_path = f'{delta_base_path}/{table_name}'
asl_root = f'{csv_base_path}/{asl_container}'
asl_changelog = f"{asl_root}/Changelog/changelog.info"

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Read the changeLog.info to identify the latest timestamp folder

# CELL ********************

from pyspark.sql.functions import split, explode, col

def get_latest_folder():
    
    # Read the file as a text file
    df = spark.read.text(asl_changelog)

    # Split each line into an array of values based on the comma delimiter
    df_split = df.select(split(col("value"), ",").alias("folder_names"))

    # Explode the array to create one row per element in the array
    df_exploded = df_split.select(explode(col("folder_names")).alias("folder_name"))

    # Order by folder_name in descending order and get the first row
    latest_row = df_exploded.orderBy(col("folder_name").desc()).first()

    # Extract the folder_name as a string
    last_folder = latest_row.folder_name if latest_row else None

    return last_folder

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

print(get_latest_folder())

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Schema for the enum table

# CELL ********************

from pyspark.sql.types import StructType, StructField, StringType, LongType, BooleanType

def generate_schema():
    schema = StructType([
        StructField("OptionSetName", StringType(), True),
        StructField("Option", LongType(), True),
        StructField("IsUserLocalizedLabel", BooleanType(), True),
        StructField("LocalizedLabelLanguageCode", LongType(), True),
        StructField("LocalizedLabel", StringType(), True),
        StructField("GlobalOptionSetName", StringType(), True),
        StructField("EntityName", StringType(), True),
        StructField("OptionName", StringType(), True),
    ])

    optionset_entity = spark.createDataFrame([], schema=schema)

    return optionset_entity


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Read the enum data

# CELL ********************

# Generate the schema from the attributes
from pyspark.sql import functions as F
from pyspark.sql.functions import current_timestamp

schema = generate_schema() 
latest_folder = get_latest_folder()
enum_path = f"{asl_root}/{latest_folder}/OptionsetMetadata/GlobalOptionsetMetadata.csv"

# Read raw CSV file
enum_CSV = spark \
    .read \
    .option('multiline', True) \
    .option('mode', 'permissive') \
    .option("quote", "\"") \
    .option("escape", "\\") \
    .option("escape", '"') \
    .option('encoding', 'UTF-8') \
    .option('charset', 'UTF-8') \
    .option('ignoreTrailingWhiteSpace', True) \
    .csv(enum_path,header=False, schema=schema.schema) 

display(enum_CSV.limit(10))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Write the metadata to table

# CELL ********************

from delta.tables import *

def saveMetadata():
    enum_CSV.write.format("delta").mode("overwrite").saveAsTable(table_name)

    return

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

saveMetadata()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# query = 'drop TABLE dataverse_etgtest_unq1d22bc67c6dbee119048000d3ad88.custtable'
# spark.sql(query)
