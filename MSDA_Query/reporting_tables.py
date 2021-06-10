from qmenta.core.platform import Auth, post, parse_response
from getpass import getpass
import pandas as pd
import numpy as np
import querries
import report_utils
import argparse
import os
import datetime

import sys
sys.path.insert(1, '../')
from data_quality.scripts.utils_cleaning import clean_data
from data_quality.scripts.utils import enhance_registry_data, enhance_forms_data
import querry_qmenta


parser = argparse.ArgumentParser(description='Arguments for reporting tables')
parser.add_argument('-update', action = 'store_true',
                    help='use to recompute QMENTA counts from server')
parser.add_argument('-except_group', type = str, default = "")
parser.add_argument('-all', action = 'store_true', help="computes all the tables for all the left out options")

args = parser.parse_args()

if args.update :

    if args.all:
        except_group = ["COV0_","COV14","forms",""]
        for group in except_group:
            querry_qmenta.querry_central_platform(group,update = True)
    else:
        querry_qmenta.querry_central_platform(args.except_group, update = True)
else:
    print("QMENTA tables not recomputed ! Uncomment below (line 35 reporting_tables.py)")
    #querry_qmenta.querry_central_platform(args.except_group, update = False)


#report_utils.germany_fix("Germany3")
#report_utils.uk_fix("UK3")

#Compute reference tables
report_utils.compute_references(directories = ["QMENTA","USA"])#,"USA7","Germany5","CEMCAT5","UK5"])

#Aggregation
report_utils.aggregate_tables(directories = ["QMENTA","USA"])#,"USA7","Germany5","CEMCAT5","UK5"])

#Generate tables for vizu
"""
report_utils.gen_tables_viz()

report_tables = []

strats = ["covid19_admission_hospital","covid19_icu_stay","covid19_outcome_death","covid19_ventilation","covid19_outcome_ventilation_or_ICU"]

for report_source in ["clinicians","patients"]:
    for features in ["age_in_cat","sex","type_current_dmt","ms_type","disease_duration_in_cat","edss_in_cat",
                    "has_comorbidities","com_cardiovascular_disease",
                    "com_diabetes","com_chronic_liver_disease"
                    ,"com_chronic_kidney_disease",
                    "com_neurological_neuromuscular",
                    "com_lung_disease",
                    "com_immunodeficiency","com_malignancy",
                    "type_current_dmt",
                    "type_last_dmt","current_dmt"]:
        r_tab = report_utils.report_table_gen(report_source, features, strats)
        report_tables+= [r_tab]
        r_tab.to_csv(f"./results/reports/report_{report_source}_{features}.csv")
"""
