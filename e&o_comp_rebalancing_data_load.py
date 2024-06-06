# Databricks notebook source
import datetime
from datetime import datetime
import pandas as pd
import pyspark.pandas as ps
import numpy as np
import os

# COMMAND ----------

# # get the current year and week number
# year, week, _day = datetime.date.today().isocalendar()

# # build the file path using the current week number
# sv_ci_file_path = f"gs://its-managed-dbx-ds-01-d-user-work-area/inventory_workstream/sv_component_inventory/{year}/sv_component_inventory_wk_{week}.csv"
# com_file_path = f"gs://its-managed-dbx-ds-01-d-user-work-area/inventory_workstream/eo_component_inventory_rebalancing/manual_input/inventory_workstream_sv_component_inventory_{year}_component_rebalancing_comments_tracking_wk_{week}.csv"
# com_file_path_last_1 = f"gs://its-managed-dbx-ds-01-d-user-work-area/inventory_workstream/eo_component_inventory_rebalancing/manual_input/inventory_workstream_sv_component_inventory_{year}_component_rebalancing_comments_tracking_wk_{week -1}.csv"
# com_file_path_last_2 = f"gs://its-managed-dbx-ds-01-d-user-work-area/inventory_workstream/eo_component_inventory_rebalancing/manual_input/inventory_workstream_sv_component_inventory_{year}_component_rebalancing_comments_tracking_wk_{week -2}.csv"
# com_file_path_last_3 = f"gs://its-managed-dbx-ds-01-d-user-work-area/inventory_workstream/eo_component_inventory_rebalancing/manual_input/inventory_workstream_sv_component_inventory_{year}_component_rebalancing_comments_tracking_wk_{week -3}.csv"
# com_file_path_last_4 = f"gs://its-managed-dbx-ds-01-d-user-work-area/inventory_workstream/eo_component_inventory_rebalancing/manual_input/inventory_workstream_sv_component_inventory_{year}_component_rebalancing_comments_tracking_wk_{week -4}.csv"


# # read the input files using the dynamically generated file paths or filter conditions
# sv_ci = pd.read_csv(sv_ci_file_path)
# sv_dns = ps.read_delta('dbfs:/user/supplier_visibility_2023_reports').filter(f"week == {week}")
# alt_map = ps.read_delta('dbfs:/user/supp_part_alt_grps_mapping')
# com_curr = pd.read_csv(com_file_path)
# com_l_1 = pd.read_csv(com_file_path_last_1)
# com_l_2 = pd.read_csv(com_file_path_last_2)
# com_l_3 = pd.read_csv(com_file_path_last_3)
# com_l_4 = pd.read_csv(com_file_path_last_4)
# com = pd.concat([com_l_4,com_l_3,com_l_2,com_l_1,com_curr],ignore_index=True)
# gsp = pd.read_csv('gs://its-managed-dbx-ds-01-d-user-work-area/inventory_workstream/eo_component_inventory_rebalancing/manual_input/gsp_names.csv')

# COMMAND ----------

# Directory path where the files are located
directory = "/Volumes/prod_catalog/shared_volume/gscr_ds/inventory_workstream/e&o_comp_rebalancing/input/sv_component_inventory/2024/"

# Get a list of all files in the directory
files = os.listdir(directory)

# Filter files based on naming convention (assuming files have "sv_component_inventory_wk_" in their names)
filtered_files = [file for file in files if "sv_component_inventory_wk_" in file]

# Sort the filtered files based on their names (assuming the format is consistent and sortable)
sorted_files = sorted(filtered_files)

# Select the most recent file
most_recent_file = sorted_files[-1]


# Construct the full path to the most recent file
file_path = os.path.join(directory, most_recent_file)

# Read the most recent file into pandas DataFrame
sv_ci = pd.read_csv(file_path)

# # Read comments file
# comments_file_path = "/Volumes/test_catalog/shared_volume/gscr_ds/inventory_workstream/e&o_comp_rebalancing/input/input_comments/2024/"
# comments_file = os.listdir(comments_file_path)[0]  # Assuming there's only one file in the directory
# comments_file_full_path = os.path.join(comments_file_path, comments_file)
# com = pd.read_csv(comments_file_full_path)

# Print or use sv_ci and com DataFrames as needed
print(file_path)
# print(comments_file_full_path)

# COMMAND ----------

week_number = int(most_recent_file.split("_wk_")[1].split(".")[0])
week_number

# COMMAND ----------

query = f"""
SELECT *
FROM prod_catalog.planning_cloud_views.supplier_visibility_2024_reports
WHERE week = {week_number}
"""

display(spark.sql(query))

# COMMAND ----------

sv_dns = spark.sql(query)
#display(sv_dns)

# COMMAND ----------

# %sql
# select * from prod_catalog.planning_cloud_views.supplier_visibility_2024_reports where week == 20

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from prod_catalog.planning_cloud_views.supp_part_alt_grps_mapping

# COMMAND ----------

alt_map = _sqldf

# COMMAND ----------

gsp = pd.read_csv('/Volumes/prod_catalog/shared_volume/gscr_ds/inventory_workstream/e&o_comp_rebalancing/input/gsp_names.csv')

# COMMAND ----------

# #reading the input files
# sv_ci = pd.read_csv("/Volumes/prod_catalog/shared_volume/gscr_ds/inventory_workstream/e&o_comp_rebalancing/input/sv_component_inventory/2024/sv_compoent_inventory_wk_20.csv")
# #sv_dns = ps.read_delta('dbfs:/user/supplier_visibility_2024_reports')
# #sv_dns = sv_dns[sv_dns.week == 14]
# ##alt_map = ps.read_delta('dbfs:/user/supp_part_alt_grps_mapping')
# #com = pd.read_csv('gs://its-managed-dbx-ds-01-d-user-work-area/inventory_workstream/eo_component_inventory_rebalancing/manual_input/input_file_component_inventory_comments wk_44.csv')
# #gsp = pd.read_csv('/Volumes/test_catalog/shared_volume/gscr_ds/inventory_workstream/e&o_comp_rebalancing/input/gsp_names.csv')

# COMMAND ----------

sv_ci.head(5)

# COMMAND ----------

sv_ci['Raw Cost'] = sv_ci['Raw Cost'].str.replace('$', '')

# COMMAND ----------

sv_ci.rename(columns = {'52 Wks Dmd Total': 'Demand Total'}, inplace = True) 


# COMMAND ----------

sv_ci.rename(columns = {'52 WKs PO Qty': 'PO Qty'}, inplace = True) 

# COMMAND ----------

sv_ci.shape

# COMMAND ----------

sv_ci.info()

# COMMAND ----------

sv_ci.columns

# COMMAND ----------

sv_ci.dtypes

# COMMAND ----------

# convert columns to lowercase and underscores
sv_ci.columns = sv_ci.columns.str.lower().str.replace(' ', '_')
gsp.columns = gsp.columns.str.lower().str.replace(' ', '_')

# filter out rows with "Applied filters" in the date column and convert date to datetime format
sv_ci = sv_ci.loc[~sv_ci["date"].str.contains("Applied filters", na=False)].copy()
sv_ci["date"] = pd.to_datetime(sv_ci["date"], format="%m/%d/%Y")

# remove commas from demand_total and po_qty columns and convert to float datatype
columns_to_convert = ["demand_total", "po_qty","zoi","soi","oh","hub","safety_stock","moq","raw_cost"]
sv_ci[columns_to_convert] = sv_ci[columns_to_convert].replace(",", "", regex=True).astype(float)

# keep only rows where site and supplier_part columns are not null for sv_ci dataframe
sv_ci = sv_ci.loc[~(sv_ci["site"].isnull() | sv_ci["supplier_part"].isnull())].copy()


# COMMAND ----------

sv_ci.supplier_part.str.replace(' ','').nunique()

# COMMAND ----------

sv_ci.supplier_part.nunique(),sv_ci.shape

# COMMAND ----------

sv_ci.info()

# COMMAND ----------

# fill missing values in zoi, soi, and hub columns with 0
cols_to_fill = ['oh','zoi', 'soi', 'hub']
sv_ci[cols_to_fill] = sv_ci[cols_to_fill].fillna(0)
# calculate zoi_soi_dem_qty, zoi_soi_dem_$, zoi_soi_po_dem_qty, and zoi_soi_po_dem_$
sv_ci['zoi_soi_dem_qty'] = (sv_ci['oh'] - sv_ci['demand_total']).astype(float)
sv_ci['zoi_soi_dem_$'] = (sv_ci['zoi_soi_dem_qty'] * sv_ci['raw_cost']).astype(float)
sv_ci['zoi_soi_po_dem_qty'] = ((sv_ci['oh'] + sv_ci['po_qty']) - sv_ci['demand_total']).astype(float)
sv_ci['zoi_soi_po_dem_$'] = (sv_ci['zoi_soi_po_dem_qty'] * sv_ci['raw_cost']).astype(float)

# COMMAND ----------

sv_ci.shape

# COMMAND ----------


#getting the alternate parts
# alt_map = alt_map.to_pandas()
alt_map = alt_map.toPandas()
sv_ci_alt = pd.merge(sv_ci,alt_map,on='part_no',how='left')
sv_ci_alt_upd = sv_ci_alt['normalized_supplier_part_no'].fillna(sv_ci_alt['supplier_part_y'], inplace=True)

#calculating grand total for normalized part no
grand_total = sv_ci_alt.groupby(['normalized_supplier_part_no'])['zoi_soi_dem_$'].sum().reset_index().rename(columns={'zoi_soi_dem_$':'grand_total'})
sv_ci_alt_gt = pd.merge(sv_ci_alt,grand_total, on=['normalized_supplier_part_no'],how='left')

op_bal = sv_ci_alt_gt.groupby(['normalized_supplier_part_no','site_name'])['zoi_soi_dem_$'].agg([('positive',lambda x:x[x>0].sum()),('negative',lambda x:x[x<0].sum())]).reset_index()
#op_bal['negative_abs'] = op_bal.negative.abs()
op_bal['norm_partno_site_total'] = op_bal.positive + op_bal.negative


#getting the positive & negative values per partno
n = op_bal.normalized_supplier_part_no.nunique()
uni_list = op_bal.normalized_supplier_part_no.unique()
pos = []
neg = []
for i in range(0,n):
    positive = op_bal[(op_bal.normalized_supplier_part_no==uni_list[i])&(op_bal.norm_partno_site_total>0)]['norm_partno_site_total'].sum()
    negative = op_bal[(op_bal.normalized_supplier_part_no==uni_list[i])&(op_bal.norm_partno_site_total<0)]['norm_partno_site_total'].sum()
    pos.append(float(positive))
    neg.append(float(negative))
data = { 'normalized_supplier_part_no':uni_list,
        'positive_f':pos,
        'negative_f':neg
}
op_bal_pn = pd.DataFrame(data)





# COMMAND ----------

#calculating the opportunity balancing
op_bal_f = pd.merge(op_bal,op_bal_pn,on='normalized_supplier_part_no',how='left')
op_bal_f['negative_abs_f']=op_bal_f.negative_f.abs()
min_val = op_bal_f[['negative_abs_f','positive_f']].min(axis=1)
op_bal_f['opportunity_balancing'] = min_val
op_bal_f_1 = op_bal_f[['normalized_supplier_part_no','opportunity_balancing']].drop_duplicates()
op_bal_f_1['opportunity_balancing_rank_no'] = op_bal_f_1.opportunity_balancing.rank(ascending=False)
op_bal_f_2 = pd.merge(op_bal_f,op_bal_f_1,on=['normalized_supplier_part_no'],how='left')
sv_ci_alt_gt_op = pd.merge(sv_ci_alt_gt,op_bal_f_2,on=['normalized_supplier_part_no','site_name'],how='left').sort_values(by='opportunity_balancing_rank_no',ascending=True).reset_index(drop=True)

# COMMAND ----------

sv_ci_alt_gt_op[sv_ci_alt_gt_op.normalized_supplier_part_no=='Memory Group 1']

# COMMAND ----------

sv_ci.shape,sv_ci_alt.shape,sv_ci_alt_gt.shape,sv_ci_alt_gt_op.shape

# COMMAND ----------

sv_ci_alt.normalized_supplier_part_no.str.replace(' ','').nunique()

# COMMAND ----------

sv_dns = sv_dns.toPandas()

# COMMAND ----------

#extracting next 2 quarters zcum delta demand from SV DNS

sv_dns1 = sv_dns.pivot_table(index=['part_no','site_name'],columns='values',values=['wk13','wk26'])
sv_dns1 = sv_dns1.reset_index(level=['part_no','site_name'])
sv_dns1.columns = sv_dns1.columns.map('_'.join)
sv_dns1.columns = sv_dns1.columns.str.replace(' ',"_")
sv_dns1 = sv_dns1[['part_no_','site_name_','wk13_Z-Cum_(Delta)','wk26_Z-Cum_(Delta)']]
sv_dns1 = sv_dns1.rename(columns={'part_no_':'part_no','site_name_':'site_name','wk13_Z-Cum_(Delta)':'wk13_zcum_delta','wk26_Z-Cum_(Delta)':'wk26_zcum_delta'})

#sv_dns1 = sv_dns1.toPandas()
sv_ci_alt_dns = pd.merge(sv_ci_alt_gt_op,sv_dns1,how='left',on=['part_no','site_name'])
sv_ci_alt_dns_gsp = pd.merge(sv_ci_alt_dns,gsp,how='left',on='site_name')
sv_ci_alt_dns_gsp['hub'] = sv_ci_alt_dns_gsp.hub.fillna(0)
sv_ci_alt_dns_gsp['demand_by_13wks'] = sv_ci_alt_dns_gsp.wk13_zcum_delta - sv_ci_alt_dns_gsp.hub
sv_ci_alt_dns_gsp['demand_by_26wks'] = sv_ci_alt_dns_gsp.wk26_zcum_delta - sv_ci_alt_dns_gsp.hub

# COMMAND ----------

sv_ci_alt_dns_gsp.shape

# COMMAND ----------

sv_ci_alt_dns_gsp_1 = sv_ci_alt_dns_gsp.dropna(subset=['site_name', 'normalized_supplier_part_no'])
sv_ci_alt_dns_gsp_1.shape

# COMMAND ----------

sv_ci.shape,sv_ci_alt.shape,alt_map.shape,sv_ci_alt_gt_op.shape,sv_dns.shape,sv_dns1.shape,sv_ci_alt_dns_gsp.shape,#sv_ci_alt_dns_gsp_2.shape

# COMMAND ----------

sv_ci_alt_dns_gsp_1.head(5)

# COMMAND ----------

#create a spark dataframe
spark_df = spark.createDataFrame(sv_ci_alt_dns_gsp_1)

# COMMAND ----------

#Output to Table
spark_df.write.mode("append").format("delta").option("mergeSchema", "true").saveAsTable("planning_cloud_views.eo_comp_rebalancing_data_load")

# COMMAND ----------

#Output to CSV
# Get the week number from the input file name
filename = most_recent_file.split("_")[4]
week_number = int(filename.split(".")[0])  # Assuming the file name format is "week_number.csv"

# Output file name with week number and current date
output_filename = f"eo_component_inventory_rebalancing_{week_number}_{datetime.now().strftime('%m-%d-%Y')}.csv"

# Output file path
output_path = os.path.join("/Volumes/prod_catalog/shared_volume/gscr_ds/inventory_workstream/e&o_comp_rebalancing/output/", output_filename)

# Save DataFrame to CSV without comments
sv_ci_alt_dns_gsp_1.to_csv(output_path, index=False)

# COMMAND ----------

# %sql
# select date, count(*) from planning_cloud_views.eo_comp_rebalancing_data_load group by date


# COMMAND ----------

# # output as csv into GCS  -- without comments
# output_path = f"/Volumes/prod_catalog/shared_volume/gscr_ds/inventory_workstream/e&o_comp_rebalancing/output/eo_component_inventory_rebalancing_20_052224.csv"
# sv_ci_alt_dns_gsp_1.to_csv(output_path,index=False)


# COMMAND ----------

#output to ZDL
# sveo = spark.createDataFrame( sv_ci_alt_dns_gsp_1_com_trans2)

# # # Show the Spark DataFrame
# sveo.show()



# sveo.write.mode("overwrite").format("delta").saveAsTable("pbi_tables_vw.eo_comp_rebalancing_{week}")#.option("overwriteSchema", "true")

# COMMAND ----------

#calculations 
import pandas as pd

# Convert Spark DataFrame to Pandas DataFrame
pandas_df = spark_df.toPandas()

# Create an empty DataFrame to store the results
results_df = pd.DataFrame()

# Initialize variables to store the total positive values
total_zoi_soi_tot_excess_val_positive = 0
total_zoi_soi_opo_tot_excess_val_positive = 0

# Loop through each row in the original DataFrame
for index, row in pandas_df.iterrows():
    # Extract values from the current row
    zoi = row['zoi']
    soi = row['soi']
    demand_total = row['demand_total']
    raw_cost = row['raw_cost']
    po_qty = row['po_qty']
    date = row['date']
    
    # Perform calculations for each row
    zoi_soi_tot_excess_qty = int((zoi + soi) - demand_total)
    zoi_soi_tot_excess_val = zoi_soi_tot_excess_qty * raw_cost  # Calculation for zoi_soi_tot_excess_val
    zoi_soi_opo_tot_excess_qty = int((zoi + soi + po_qty) - demand_total)  # Calculation for zoi_soi_opo_tot_excess_qty
    zoi_soi_opo_tot_excess_val = zoi_soi_opo_tot_excess_qty * raw_cost  # Calculation for zoi_soi_opo_tot_excess_val
    
    # Create a dictionary with the calculated results
    results_dict = {
        'date': date,
        'zoi_soi_tot_excess_qty': zoi_soi_tot_excess_qty,
        'zoi_soi_tot_excess_val': zoi_soi_tot_excess_val,
        'zoi_soi_opo_tot_excess_qty': zoi_soi_opo_tot_excess_qty,
        'zoi_soi_opo_tot_excess_val': zoi_soi_opo_tot_excess_val   
    }
    
    # Append the results to the results DataFrame
    results_df = results_df.append(results_dict, ignore_index=True)

# Display the final results DataFrame
print(results_df)


# COMMAND ----------

results_spark_df = spark.createDataFrame(results_df)

# COMMAND ----------


sum = results_df.loc[results_df['zoi_soi_tot_excess_val']>0,'zoi_soi_tot_excess_val'].sum()
sum


# COMMAND ----------

sum1 = results_df.loc[results_df['zoi_soi_opo_tot_excess_val']>0,'zoi_soi_opo_tot_excess_val'].sum()
sum1


# COMMAND ----------

# Create a dictionary with the total positive values
total_df = pd.DataFrame()
total_dict = {
    'total_zoi_soi_tot_excess_val_positive': sum,
    'total_zoi_soi_opo_tot_excess_val_positive': sum1
}

# Append the total positive values to the total DataFrame
total_df = total_df.append(total_dict, ignore_index=True)

# Display the final total DataFrame
print(total_df)

# COMMAND ----------

#Oppurtunity Balancing\
Opp_df = pd.DataFrame()
Opp_df = pandas_df[['normalized_supplier_part_no', 'opportunity_balancing_x']]
Opp_df 

# COMMAND ----------

Opp_df = Opp_df.drop_duplicates(subset=['normalized_supplier_part_no', 'opportunity_balancing_x'])
Opp_df

# COMMAND ----------

total_opportunity_balancing_x = Opp_df['opportunity_balancing_x'].sum()
total_opportunity_balancing_x

# COMMAND ----------

date_column = pandas_df['date']
date_column

# COMMAND ----------

# Create a dictionary with the total positive values
total_df = pd.DataFrame()
total_dict = {
    'Week': week_number,
    'Date': date_column[0],
    'Total_ZOI_SOI_tot_excess_val_positive': sum,
    'Total_ZOI_SOI_opo_tot_excess_val_positive': sum1,
    'OpportunityBalancing':total_opportunity_balancing_x,
    'transferred_qty_actual':'',
    'transferred_val_balance':''
}

# Append the total positive values to the total DataFrame
total_df = total_df.append(total_dict, ignore_index=True)

# Display the final total DataFrame
print(total_df)

# COMMAND ----------

total_df

# COMMAND ----------

total_df = spark.createDataFrame(total_df)

# COMMAND ----------

total_df.write.format("delta").option("mergeSchema", "true").mode("append").saveAsTable("planning_cloud_views.Reports_loads") 

# COMMAND ----------

# %sql
# SELECT Week, Date, 
#        Total_ZOI_SOI_tot_excess_val_positive, 
#        Total_ZOI_SOI_opo_tot_excess_val_positive, 
#        OpportunityBalancing,
#        NULL AS transferred_qty_actual,
#        NULL AS transferred_val_balance
# FROM planning_cloud_views.Reports_loads
# UNION ALL
# SELECT Week, Date, 
#        Total_ZOI_SOI_tot_excess_val_positive, 
#        Total_ZOI_SOI_opo_tot_excess_val_positive, 
#        OpportunityBalancing,
#        transferred_qty_actual,
#        transferred_val_balance
# FROM planning_cloud_views.Reports_comments;


# COMMAND ----------

# %sql
# --CREATE TABLE planning_cloud_views.Reports_combined AS
# INSERT INTO TABLE planning_cloud_views.Reports_combined
# SELECT Week, Date, 
#        Total_ZOI_SOI_tot_excess_val_positive, 
#        Total_ZOI_SOI_opo_tot_excess_val_positive, 
#        OpportunityBalancing,3
#        NULL AS transferred_qty_actual,
#        NULL AS transferred_val_balance
# FROM planning_cloud_views.Reports_loads

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO TABLE planning_cloud_views.Reports_combined
# MAGIC SELECT Week, Date, 
# MAGIC        Total_ZOI_SOI_tot_excess_val_positive, 
# MAGIC        Total_ZOI_SOI_opo_tot_excess_val_positive, 
# MAGIC        OpportunityBalancing,
# MAGIC        NULL AS transferred_qty_actual,
# MAGIC        NULL AS transferred_val_balance
# MAGIC FROM planning_cloud_views.Reports_loads
# MAGIC WHERE Week = (SELECT MAX(Week) FROM planning_cloud_views.Reports_loads)

# COMMAND ----------


