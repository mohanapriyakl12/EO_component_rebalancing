# Databricks notebook source
import pandas as pd
import pyspark.pandas as ps
import numpy as np
import os

# COMMAND ----------

#path to the directory containing files
directory = "/Volumes/prod_catalog/shared_volume/gscr_ds/inventory_workstream/e&o_comp_rebalancing/input/part_grouping_files/"

#list all files in the directory
files = os.listdir(directory)

#fecthing the recent file from the directory
most_recent_file = files[-1]

# Construct the full path to the most recent file
file_path = os.path.join(directory, most_recent_file)

# COMMAND ----------

file_path

# COMMAND ----------

#reading the most_recent_files into pandas dataframe
part_grouping = pd.read_csv(file_path)
part_grouping

# COMMAND ----------

if part_grouping['part_no'].duplicated().any():
    print("File has duplicate values cannot proceed further")
    sys.exit()
else:
    print("No Duplicates!")




# COMMAND ----------

part_grouping_df = pd.read_csv(file_path)

# COMMAND ----------

part_grouping_df

# COMMAND ----------

part_grouping_df =spark.createDataFrame(part_grouping_df)

# COMMAND ----------

part_grouping_df.write.format("delta").option("mergeSchema","true").mode("overwrite").saveAsTable("planning_cloud_views.supp_part_alt_grps_mapping")

# COMMAND ----------


