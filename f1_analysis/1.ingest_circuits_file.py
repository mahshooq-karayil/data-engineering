# Databricks notebook source
# MAGIC %md
# MAGIC ###Ingest circuits.csv file

# COMMAND ----------

dbutils.widgets.text('p_data_source','')
v_data_source = dbutils.widgets.get('p_data_source')

# COMMAND ----------

# MAGIC %run "../includes/configuration"

# COMMAND ----------

# MAGIC %run "../includes/common_functions"

# COMMAND ----------

# MAGIC %md
# MAGIC #####Step 1 - Read the CSV file using the spark dataframe reader

# COMMAND ----------

from pyspark.sql.types import StructType,StructField,IntegerType,StringType,DoubleType

# COMMAND ----------

circuits_schema=StructType(fields=[StructField('circuitid',IntegerType(),False),
                                    StructField('circuitRef',StringType(),True),
                                    StructField('name',StringType(),True),
                                    StructField('location',StringType(),True),
                                    StructField('country',StringType(),True),
                                    StructField('lat',DoubleType(),True),
                                    StructField('lng',DoubleType(),True),
                                    StructField('alt',IntegerType(),True),
                                    StructField('url',StringType(),True)]
                            )

# COMMAND ----------

circuits_df = spark.read.csv(f"{raw_folder_path}/circuits.csv",header=True,schema=circuits_schema)


# COMMAND ----------

circuits_df.describe()

# COMMAND ----------

# MAGIC %md
# MAGIC #####Step 2 -Select only the required columns

# COMMAND ----------

from pyspark.sql.functions import lit,col

# COMMAND ----------

circuits_selected = circuits_df.select(col('circuitid'),col('circuitRef'),
                                       col('name'),col('location'),
                                       col('country'),col('lat'),
                                       col('lng'),col('alt'))

# COMMAND ----------

# MAGIC %md
# MAGIC #####Step 3 - Rename the columns as required

# COMMAND ----------

circuits_renamed_df = circuits_selected.withColumnRenamed('circuitId','circuit_id') \
    .withColumnRenamed('circuitRef','circuit_ref') \
    .withColumnRenamed('lat','latitude') \
    .withColumnRenamed('lng','longitude') \
    .withColumnRenamed('alt','altitude') \
    .withColumn('data_source',lit(v_data_source))

# COMMAND ----------

# MAGIC %md
# MAGIC #####Step 4 - Add ingestion date to the dataframe

# COMMAND ----------

circuits_final_df = add_ingestion_date(circuits_renamed_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 4 - Write the output to processed container in parquet file

# COMMAND ----------

circuits_final_df.write.mode('overwrite').parquet(f"{processed_folder_path}/circuits")

# COMMAND ----------

circuits_final_df.write.mode('overwrite').format('parquet').saveAsTable("f1_processed.circuits")

# COMMAND ----------

dbutils.notebook.exit('Success')
