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

# PARAMETERS CELL ********************

storage_account = "stgtwdemo002"
asl_container = "dataverse-twdemo002-unqba7b05a00b3af0119a43002248932"
table_name = 'custtable'
reload_table = False

base_path = 'Files/d365'
watermark_name = '_watermark'
''' key_vault = 'kv-twdemo001'
tenant_id = 'fabric-integration-tenant'
app_id = 'fabric-integration-appid'
app_secret = 'fabric-integration-secret' '''


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

watermark_column = 'lastload'
asl_model = "model.json"
rename_columns = {
    'Id':'sl_id',
    'fno_id':'id'
}
exclude_metacolumns = [ 'sl_id', 'versionnumber', 'tableid', 'modifiedtransactionid', 'createdtransactionid']
ignore_metacolumns = [ '_sysProcessedDateTime', 'SinkCreatedOn', '_sysCreatedDateTime', 'SinkModifiedOn' ]


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

delta_base_path = f'{base_path}/delta'
csv_base_path = f'{base_path}/csv'
delta_path = f'{delta_base_path}/{table_name}'
watermark_location = f'{delta_path}/{watermark_name}'
asl_root = f'{csv_base_path}/{asl_container}'
asl_changelog = f"{asl_root}/Changelog/changelog.info"


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

print(f"Delta path: {delta_path}")
print(f"Watermark location: {watermark_location}")
print(f"ASL Root location: {asl_root}")
print(f"ASL Changelog location: {asl_changelog}")
print(f"Table: {table_name}")
print(f"Reload table: {reload_table}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

spark.conf.set("spark.sql.legacy.timeParserPolicy", "LEGACY")
spark.conf.set("spark.databricks.delta.schema.autoMerge.enabled", "true")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from datetime import datetime
water_mark = None
if not reload_table:
    try:
        wm = spark.read.parquet(watermark_location).select(watermark_column)
        latest_wm = wm.limit(1).collect()
        water_mark = latest_wm[0][0]
    except:
        pass
print(water_mark)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def get_allFolders():
    folders = []
    changelog_df = spark.read.text(asl_changelog)
    try:
        latest_folder = changelog_df.collect()[0][0]
    except:
        latest_folder = None
    all_folders = mssparkutils.fs.ls(asl_root)
    folders = [f.name for f in all_folders if (water_mark == None or f.name > water_mark) and (latest_folder == None or f.name < latest_folder) and not f.name in ('Changelog', 'EntitySyncFailure', 'Microsoft.Athena.TrickleFeedService', 'model.json') ]
    return folders

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def get_folders_forTable():
    process_folders = []
    folders = get_allFolders()
    for folder in folders:
        include = False
        path = f'{asl_root}/{folder}/{table_name}'
        if mssparkutils.fs.exists(path):
            files = mssparkutils.fs.ls(path)
            for file in files:
                if file.name.lower().endswith('.csv'):
                    include = True
                    break
        if include:
            process_folders.append(folder)

    return process_folders


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

process_folders = get_folders_forTable()
print(process_folders)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

if len(process_folders) == 0:
    mssparkutils.notebook.exit(1)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

csv_paths = f"{asl_root}/{{{','.join(process_folders)}}}/{table_name}/*.csv"
print(f"CSV paths: {csv_paths}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql.functions import split, explode, col, size, to_json
import json
def extract_raw_schema():
    path = f'{asl_root}/{asl_model}'
    df = spark.read.json(path)
    exploded_df = df.select(explode(col('entities')).alias('tables'))
    filtered_df = exploded_df.filter(col('tables.name') == table_name)
    schema_df = filtered_df.select(col('tables.attributes').alias('schema'))
    return schema_df.toJSON().first()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

raw_schema = extract_raw_schema()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql.types import (
    StructType, StructField, TimestampType, DateType, BooleanType, IntegerType, LongType, FloatType, DoubleType, DecimalType, StringType
)

def field_already_exists(field_name, fields):
    for field in fields:
        if field.name.lower() == field_name.lower():
            return True
    return False

def generate_cdm_schema(raw_schema):
    tables_json = json.loads(raw_schema)

    fields = []
    for field in tables_json['schema']:
        raw_dataType = field['dataType'].lower()
        field_name = field['name']
        if field_already_exists(field_name, fields):
            suffix = 1
            new_field_name = f'{field_name}{suffix}'

            while field_already_exists(new_field_name, fields):
                suffix +=1
                new_field_name = f'{field_name}{suffix}'
            
            field_name = new_field_name
        field_name = rename_columns.get(field_name, field_name)

        if field_name in {'SinkCreatedOn', 'SinkModifiedOn'} and raw_dataType == 'datetime':
            fields.append(StructField(field_name, StringType(), True))
        elif raw_dataType in {'datetime', 'datetimeoffset'}:
            fields.append(StructField(field_name, TimestampType(), True))
        elif raw_dataType == 'date':
            fields.append(StructField(field_name, DateType(), True))
        elif raw_dataType == 'boolean':
            fields.append(StructField(field_name, BooleanType(), True))
        elif raw_dataType in {'int16', 'int32'}:
            fields.append(StructField(field_name, IntegerType(), True))
        elif raw_dataType == 'int64':
            fields.append(StructField(field_name, LongType(), True))
        elif raw_dataType == 'float':
            fields.append(StructField(field_name, FloatType(), True))
        elif raw_dataType == 'double':
            fields.append(StructField(field_name, DoubleType(), True))        
        elif raw_dataType == 'decimal':
            fields.append(StructField(field_name, DecimalType(38, 6), True))      
        else:
            fields.append(StructField(field_name, StringType(), True))      
            if raw_dataType not in {'string', 'guid'}:
                print(f'Unknown datatype : {raw_dataType}')

    cdm_schema = StructType(fields)

    return spark.createDataFrame([], cdm_schema)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

cdm_schema = generate_cdm_schema(raw_schema)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

source_df = spark.read \
    .option('multiline', True) \
    .option('mode', 'permissive') \
    .option('quote', "\"") \
    .option('escape', "\\") \
    .option('escape', '"') \
    .option('encoding', 'UTF-8') \
    .option('charset', 'UTF-8') \
    .option('ignoreTrailingWhiteSpace', True)\
    .option('header', False) \
    .option('timestampFormat', "yyyy-MM-dd'T'HH:mm:ss.SSS") \
    .option('lineSep', "\r\n") \
    .csv(csv_paths, header = False, schema=cdm_schema.schema)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql.functions import when, col, expr
table_id = 'cast(conv(substring(sl_id, 0, 8), 16, 10) as long)'
source_df = source_df.withColumn('tableid', when(col('tableid').isNull(),expr(table_id)).otherwise(col('tableid')))


record_id = 'cast(conv(concat(substring(sl_id,35,2),substring(sl_id,33,2),substring(sl_id,31,2),substring(sl_id,29,2),substring(sl_id,27,2),substring(sl_id,25,2),substring(sl_id,22,2),substring(sl_id,20,2)),16,10) as long)'
source_df = source_df.withColumn('recid', when(col('recid').isNull(),expr(record_id)).otherwise(col('recid')))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql.functions import to_timestamp
source_df = source_df.withColumn('SinkModifiedOn', to_timestamp(source_df['SinkModifiedOn'], "M/d/yyyy h:mm:ss a"))
source_df = source_df.withColumn('SinkCreatedOn', to_timestamp(source_df['SinkCreatedOn'], "M/d/yyyy h:mm:ss a"))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql.functions import col, lit

sql_query = f"SELECT OptionSetName as Attribute, Option as EnumValue, OptionName as EnumId FROM _enum_translations where EntityName = '{table_name}' order by OptionSetName, Option"

entity_enum = df = spark.sql(sql_query)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql.functions import col, lit, collect_list, concat
import functools

entityEnum_list = entity_enum \
    .groupBy("Attribute") \
    .agg(collect_list(concat(lit('when '), 'EnumValue', lit(" then '"), 'EnumId', lit("'"))) \
    .alias("enum_values")) \
    .collect()

table_enums = []
for ee in entityEnum_list:
    enum_values = functools.reduce(lambda a, b: f"{a} {b}", ee[1])
    #enum_values = enum_values.strip()
    table_enums += [[ee[0], f"case {ee[0]} {enum_values} end"]]

print(f"{table_name} contains {len(table_enums)} enums") 

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql.functions import expr

print('Decoding enums')

for te in table_enums:
    #print(f"{te[0]}: {te[1]}")
    source_df = source_df.withColumn(te[0], expr(te[1]))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql.window import Window
from pyspark.sql.functions import row_number, desc, col, lit, coalesce

def get_latest_version(df):
    # Define the window specification for dense rank
    windowSpec = Window.partitionBy("RECID").orderBy(desc('VersionNumber'), desc('SinkModifiedOn'), desc('RecVersion'))
    df = df \
        .withColumn("_row_number", row_number().over(windowSpec)) \
        .filter("_row_number = 1") \
        .drop("_row_number")

    return df

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql.functions import col, current_date, current_timestamp
import functools

merge_columns = [col(f"t.{c}").eqNullSafe(col(f"s.{c}")) for c in (["RECID"])]
merge_condition = functools.reduce(lambda a, b: a & b, merge_columns)

print(merge_condition)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def drop_metacolumns(df):
    return df.drop(*exclude_metacolumns)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from delta.tables import *
from pyspark.sql.functions import col, current_date, current_timestamp, hash, concat, expr
import functools

def make_comparecondition(dt):

    existing_columns = [c.name for c in dt.toDF().schema]

    compareColumns_S = [f"s.{c.name}" for c in source_df.schema if c.name.lower() not in map(str.lower, ["RECID"] + exclude_metacolumns + ignore_metacolumns)]
    compareColumns_T = [f"t.{c.name}" for c in source_df.schema if c.name.lower() in map(str.lower, existing_columns) and c.name.lower() not in map(str.lower, ["RECID"] + exclude_metacolumns + ignore_metacolumns)]


    hashS = expr('hash(' + functools.reduce(lambda a, b: a + ',' + b, compareColumns_S) + ')')
    hashT = expr('hash(' + functools.reduce(lambda a, b: a + ',' + b, compareColumns_T) + ')')

    compare_condition = hashS.eqNullSafe(hashT)

    #print(compare_condition)

    return compare_condition


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql.functions import col, lit, coalesce

delete_condition = (coalesce(col(f"s.IsDelete"), lit(False)) == lit(True))
insert_condition = ~delete_condition


print(f"Insert: {insert_condition}")
print(f"Delete: {delete_condition}")    

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql.functions import current_timestamp

merge_source = source_df.withColumn('_sysProcessedDateTime', current_timestamp())
merge_source = source_df.withColumn('_sysCreatedDateTime', current_timestamp())

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def add_missing_columns(destination_table, batchDF):
    
    # Get columns in target delta table
    existing_columns = [field.name.lower() for field in spark.table(destination_table).schema.fields] 
    
    # Get columns in source that don't exist in the target
    new_columns = [field for field in batchDF.schema.fields if field.name.lower() not in existing_columns]
    for field in new_columns:
        spark.sql(f"ALTER TABLE {destination_table} ADD COLUMNS (`{field.name}` {field.dataType.simpleString()})")
        print(f"ALTER TABLE {destination_table} ADD COLUMNS (`{field.name}` {field.dataType.simpleString()})")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import datetime
from pyspark.sql.functions import col, lit, coalesce
import functools
from delta.tables import *
from pyspark.sql.window import Window
from pyspark.sql.functions import dense_rank, desc


def merge_write(merge_source):

    #merge_source = renameMetaDataColumns(merge_source)
    merge_source = get_latest_version(merge_source)
    merge_source = drop_metacolumns(merge_source)
    
    if reload_table or not spark.catalog.tableExists(table_name):
        print('Overwrite')
        merge_source = merge_source.alias('s').filter(insert_condition)
        dfw = merge_source.write.format("delta").mode("overwrite").option("header", True).option("delta.enableChangeDataFeed", "true").option('overwriteSchema', "true").saveAsTable(f'{table_name}')



    else:
        print('merge')
        
        # Add any columns that dont exist in delta that exist in source
        add_missing_columns(f'{table_name}', merge_source)
            
        mergeTable = DeltaTable.forName(spark, table_name)
        existing_columns = [c.name for c in mergeTable.toDF().schema]

        upsert_condition = ~make_comparecondition(mergeTable)

        # Don't update primary key or createdTimeStamp columns
        update_columns = { f"t.{c.name}" : f"s.{c.name}" for c in merge_source.schema if c.name.lower() in map(str.lower,existing_columns) and c.name.lower() not in map(str.lower,["RECID"] + ["_sysCreatedDateTime"]) }

        print(f"existingColumns: {existing_columns}")
        print(f"updateColumns: {update_columns}")  

        # Process inserts/updates
        m = mergeTable.alias('t') \
            .merge(merge_source.alias('s').filter(~delete_condition), merge_condition) \
            .whenMatchedUpdate(upsert_condition, set=update_columns) \
            .whenNotMatchedInsertAll() \
            .execute()

        # Process deletes
        m = mergeTable.alias('t') \
            .merge(merge_source.alias('s').filter(delete_condition), merge_condition)



        print('delete')

        m = m.whenMatchedDelete()

        m.execute()

    print(f'End time: {datetime.datetime.now()}')


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

merge_write(source_df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DecimalType, TimestampType
from datetime import datetime

#if loadType == LoadType.full.name:
schema = StructType(
    [
        StructField('tableName', StringType()),
        StructField(watermark_column, StringType(), False),
        StructField('_sysProcessDateTime', TimestampType(), False)
    ]
)

wm = spark.createDataFrame([(table_name, process_folders[-1], datetime.now())], schema)
wm.write.parquet(watermark_location, 'overwrite')

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
