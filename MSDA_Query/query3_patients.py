import pandas as pd
import numpy as np
import querries
import os
import sys
sys.path.insert(1, '../')

from data_quality.scripts.utils_cleaning import clean_data
from data_quality.scripts.utils import enhance_data, clean_dates

df = pd.read_csv("./Data/msda_data_patients.csv")

#Needed to interface with cleaning scripts
df["id"] = np.arange(df.shape[0])
date_cols = [c for c in df.columns if "date" in c]
for date_col in date_cols:
    df[date_col] = pd.to_datetime(df[date_col],errors="coerce")

#Clean
print("Cleaning data ....")
df = clean_data(df,None,None)
df = clean_dates(df)

#Augment
print("Creating new variables ....")

df["report_source"]="patients"
df = enhance_data(df)
df["secret_name"] = np.arange(df.shape[0])
#Compute the tables
print("Computing tables ....")
querries.compute_tables(df, report_source = "patients")
print("Done !")


os.makedirs("./Outputs", exist_ok=True)
file_name = "patients_query3_bmi_in_cat2"
outcome_types = ["covid19_admission_hospital","covid19_icu_stay","covid19_ventilation","covid19_outcome_death","covid19_outcome_ventilation_or_ICU", "covid19_outcome_levels_1", "covid19_outcome_levels_2"]
for outcome_type in outcome_types:
    df = pd.read_csv(f"./results/{file_name}_{outcome_type}.csv")
    if outcome_type=="covid19_admission_hospital":
        variables_list = ["dmt_type_overall","age_in_cat","ms_type2","sex_binary","edss_in_cat2"]
        variables_list += ["covid19_diagnosis"]
        variables_list += [outcome_type]
    else:
        variables_list = [outcome_type]
    for variable in variables_list:
       # print(df.groupby(variable)["secret_name"].sum()) #tables to return.
       result = (df.groupby(variable)["secret_name"].sum())
       result.to_json(f"./Outputs/patients-{variable}.json")

with open("DoneP.txt", "w") as file:
    file.write("query3_patients Process is Done")
