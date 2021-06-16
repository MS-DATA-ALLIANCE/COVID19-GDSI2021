from qmenta.core.platform import Auth, post, parse_response
from getpass import getpass
import pandas as pd
import numpy as np
import querries
import report_utils
import argparse
import os
import datetime
import shutil

import sys
sys.path.insert(1, '../')
from data_quality.scripts.utils_cleaning import clean_data
from data_quality.scripts.utils import enhance_registry_data, enhance_forms_data, clean_dates



def get_subjects_data(project_id, auth_obj):
    r = post(auth_obj, "/patient_manager/get_patient_list",
            {"_pid":project_id},
            timeout=600.0)

    data_trans = [{
            "id": record["_id"],
            "secret_name":record["patient_secret_name"],
            **{
                k[3:]:record[k]
                for k in record
                if k[:3] == "md_"
            }
    } for record in parse_response(r)]

    for r in data_trans:
        for k in r:
            if isinstance(r[k], dict):
                try:
                    r[k] = datetime.datetime.fromtimestamp(r[k]["$date"]/1000.0)
                except:
                    r[k] = None

    return data_trans

def compute_json_report(report_source):
    print(f"Writing report files for QMENTA {report_source}.... ")
    #if os.path.isdir(f"./Outputs_QMENTA/{report_source}"):
    #    shutil.rmtree(f"./Outputs_QMENTA/{report_source}")
        
    os.makedirs("./Outputs_QMENTA", exist_ok=True)
    file_name = f"{report_source}_query3_bmi_in_cat2"
    outcome_types = ["covid19_admission_hospital","covid19_icu_stay","covid19_ventilation","covid19_outcome_death","covid19_outcome_ventilation_or_ICU", "covid19_outcome_levels_1", "covid19_outcome_levels_2"]
    for outcome_type in outcome_types:
        df = pd.read_csv(f"./results/QMENTA/{file_name}_{outcome_type}.csv")
        df["general_source"] = df.source.apply(lambda x : x.split("_")[0])
        for source in df.general_source.unique():
            os.makedirs(f"./Outputs_QMENTA/{source}", exist_ok=True)
            df_ = df.loc[df.general_source==source]
            if outcome_type=="covid19_admission_hospital":
                variables_list = ["dmt_type_overall","age_in_cat","ms_type2","sex_binary","edss_in_cat2"]
                variables_list += ["covid19_diagnosis"]
                variables_list += [outcome_type]
            else:
                variables_list = [outcome_type]
            for variable in variables_list:
               # print(df.groupby(variable)["secret_name"].sum()) #tables to return.
               result = (df_.groupby(variable)["secret_name"].sum())
               result.to_json(f"./Outputs_QMENTA/{source}/{report_source}-{variable}.json")

    with open("DoneC.txt", "w") as file:
        file.write(f"JSON report generated for QMENTA {report_source}")
    print("Done")



def querry_central_platform(except_group = "", update = False):

    if update: # Fetch new data from the platform.
        # the id of the project to be used
        project_id_reg = 3202
        project_id_forms = 3150

        #the ID of the registry project is 3202
        #the ID of the public forms data is 3150

        # base url to connect to the central platform
        base_url = "https://platform.qmenta.com"
        # PUT YOUR USERNAME (EMAIL) HERE
        username = input("QMENTA - username :  ")
        # you will be asked for your password here
        password = getpass()

        # creation of authentication object
        auth_obj = Auth.login(username, password, base_url)

        # method to fetch the subjects data
        def get_subjects_data(project_id,auth_obj):
            r = post(auth_obj, "/patient_manager/get_patient_list",
                    {"_pid":project_id},
                    timeout=600.0)

            data_trans = [{
                    "id": record["_id"],
                    "secret_name":record["patient_secret_name"],
                    **{
                        k[3:]:record[k]
                        for k in record
                        if k[:3] == "md_"
                    }
            } for record in parse_response(r)]

            for r in data_trans:
                for k in r:
                    if isinstance(r[k], dict):
                        try:
                            r[k] = datetime.datetime.fromtimestamp(r[k]["$date"]/1000.0)
                        except:
                            r[k] = None

            return data_trans

        data_reg = get_subjects_data(project_id_reg,auth_obj)
        data_forms = get_subjects_data(project_id_forms, auth_obj)
        print(f"Total number of subjects: {len(data_reg)}")
        print(f"Total number of subjects: {len(data_forms)}")

        df_forms = pd.DataFrame(data_forms)
        df_reg = pd.DataFrame(data_reg)

        df_forms = clean_dates(df_forms)
        df_reg = clean_dates(df_reg)

        df_reg = clean_data(df_reg, None, None, None)
        df_forms = clean_data(df_forms, None, None, None)

        df_reg = enhance_registry_data(df_reg)
        df_forms = enhance_forms_data(df_forms)

        if except_group=="forms":
            df = df_reg.copy()
        else:
            df = pd.concat([df_reg,df_forms],sort = True)
        df.drop_duplicates(subset = ["secret_name"], inplace = True)

        df_clinicians = df.loc[df.report_source.str.contains("clinicians")].copy()
        df_patients = df.loc[df.report_source.str.contains("patients")].copy()

        if (except_group == ""):
            df_clinicians.to_csv("./results/QMENTA/df_clinicians.csv",index = False)
            df_patients.to_csv("./results/QMENTA/df_patients.csv",index = False)

    else:
        print("Extracting QMENTA tables from local copy...")
        df_clinicians = pd.read_csv("./results/QMENTA/df_clinicians.csv")
        df_patients = pd.read_csv("./results/QMENTA/df_patients.csv")

    #print((df_clinicians.loc[df_clinicians.secret_name.str.contains(except_group)]).shape)
    #print((df_patients.loc[df_patients.secret_name.str.contains(except_group)]).shape)

    if (except_group != "") and (except_group!="forms"):
        df_clinicians = df_clinicians.loc[~df_clinicians.secret_name.str.contains(except_group)].copy()
        df_patients = df_patients.loc[~df_patients.secret_name.str.contains(except_group)].copy()

        #print(df_clinicians.loc[df_clinicians.secret_name.str.contains(except_group)].shape)
        #print(df_patients.loc[df_patients.secret_name.str.contains(except_group)].shape)

    outfile = f"QMENTA{except_group}/"
    querries.compute_tables(df_clinicians, report_source = "clinicians",outfile = outfile, central_platform = True)
    querries.compute_tables(df_patients, report_source = "patients",outfile = outfile, central_platform = True)

    compute_json_report("clinicians")
    compute_json_report("patients")
