import os
import pandas as pd
import warnings

warnings.filterwarnings("ignore")
from urllib.request import urlretrieve
import zipfile
import json
import os
import shutil

def get_partition(csv_file):
  # get the partition from the file name
  return "-".join(csv_file.rsplit("-",3)[-3:]).split(".")[0]

def get_categories_files_dict(vertical):
  category_files_dict = {"all":{},"us":{},"non_us":{},"us_zip":{},"us_county":{}}
  
  def update_dict(parent_key:str,child_partition_key:str,data_type:str,update_path:str):
    if child_partition_key not in category_files_dict[parent_key]:
      category_files_dict[parent_key].update({child_partition_key:{}})
      category_files_dict[parent_key][child_partition_key].update({data_type:update_path})
    else:
      category_files_dict[parent_key][child_partition_key].update({data_type:update_path})
    return category_files_dict
  
  for csv_file in os.listdir(vertical):
    partition_name = get_partition(csv_file)
    if "date" in csv_file: data_type = "date"
    elif "provenance" in csv_file: data_type = "provenance"
    elif "value" in csv_file: data_type = "value"
    if "_all_" in csv_file:  
      category_files_dict = update_dict("all",partition_name,data_type,csv_file)
    elif "_us_" in csv_file and "non" not in csv_file:
      category_files_dict = update_dict("us",partition_name,data_type,csv_file)
    elif "_non_us_" in csv_file:
      category_files_dict = update_dict("non_us",partition_name,data_type,csv_file)
    elif "_zip_" in csv_file:
      category_files_dict = update_dict("us_zip",partition_name,data_type,csv_file)
    elif "_county_" in csv_file:
      category_files_dict = update_dict("us_county",partition_name,data_type,csv_file)
  return category_files_dict



def get_stats_var(row):
    stats_var_list = []
    place_name = row["place_name"]
    place_dcid = row["place_dcid"]
    place_type = row["place_type"]

    for col,val in zip(row[3:].index.tolist(),row[3:].tolist()):
        col = col.rsplit("_",1)[0]
        stats_val,stats_provenance,stats_date = val.split("::")
        if stats_val=="nan":
            stats_val = 0
        else:
            stats_val = float(stats_val)
        if stats_date=="nan":
            stats_date = 0
        else:
            stats_date = int(float(stats_date))
        if stats_provenance=="nan":
            stats_provenance = ""
        stats_var_list.append({"stats_var_dcid":col,"place_dcid":place_dcid,"place_name":place_name,"place_type":place_type,"place_category":"all","stats_var_latest_date":int(float(stats_date)),"stats_var_value":float(stats_val),"stats_provenance":stats_provenance,"vertical":"agriculture"})    
    return stats_var_list

def get_stats_var_list(value_csv,date_csv,provenance_csv):
    merged_df = value_csv.merge(date_csv, on=['place_name', 'place_dcid', 'place_type'], suffixes=('_value', '_date'))
    merged_df = merged_df.merge(provenance_csv, on=['place_name', 'place_dcid', 'place_type'], suffixes=('', '_provenance'))

    # Identify non-common columns
    common_columns = ['place_name', 'place_dcid', 'place_type']
    non_common_columns = [col for col in value_csv.columns if col not in common_columns]

    # Combine the values for each non-common column
    for col in non_common_columns:
        merged_df[f"{col}_val"] = merged_df[f"{col}_value"].astype(str) + "::" +  merged_df[f"{col}"].astype(str) + "::" + merged_df[f"{col}_date"].astype(str)
        # Drop the individual columns used for merging
        merged_df = merged_df.drop(columns=[f"{col}_value", f"{col}_date",col])
    stats_var_list = []
    for index, row in merged_df.iterrows():
        curr_row_stats_var_list = get_stats_var(row)
        stats_var_list.extend(curr_row_stats_var_list)
    return stats_var_list

def get_vertical_stats_var(category_files_dict):
  all_stats_var = []
  for category, partition_vals in category_files_dict.items():
      for partition, data_type_vals in partition_vals.items():
          date_csv = pd.read_csv(os.path.join("agriculture",data_type_vals['date']))
          provenance_csv = pd.read_csv(os.path.join("agriculture",data_type_vals['provenance']))
          value_csv = pd.read_csv(os.path.join("agriculture",data_type_vals['value']))
          all_stats_var.extend(get_stats_var_list(value_csv,date_csv,provenance_csv))
  return all_stats_var

def save_stats_var(vertical:str):
  # Retrieve the file
  print(f"{vertical}-----------")
  urlretrieve(f"https://storage.googleapis.com/relational_tables/{vertical}.zip",f"{vertical}.zip")
  print("Downloaded")
  # Extract files

  with zipfile.ZipFile(f"{vertical}.zip", 'r') as zip_ref:
    zip_ref.extractall(vertical)
  print("Extracted")
  category_files_dict = get_categories_files_dict(vertical)
  print("Getting stats var")
  all_stats_var = get_vertical_stats_var(category_files_dict)
  print(len(all_stats_var))
  with open(f"/content/drive/MyDrive/DataCommons/{vertical}.json", "w") as final:
    json.dump(all_stats_var, final)
  shutil.rmtree(vertical)
  os.remove(f"{vertical}.zip")
  return all_stats_var

if __name__ == '__main__':
    verticals = ["agriculture","crime","climate","demographics","economics","education","employment","energy","health","household"]

    import concurrent.futures

    with concurrent.futures.ProcessPoolExecutor(max_workers=3) as executor:
        executor.map(save_stats_var,verticals)