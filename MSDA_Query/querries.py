import pandas as pd
import numpy as np
#from pandas_profiling import ProfileReport
import os
import itertools


def compute_tables(df_in, report_source, outfile = "",central_platform = False):

    os.makedirs("./results", exist_ok=True)

    stratification_cols = ["covid19_admission_hospital","covid19_icu_stay","covid19_ventilation","covid19_outcome_death","covid19_outcome_ventilation_or_ICU", "covid19_outcome_levels_1", "covid19_outcome_levels_2"]

    main_vars = ["dmt_type_overall","age_in_cat","ms_type2","sex_binary","edss_in_cat2"]
    extra_vars = ["current_or_former_smoker","bmi_in_cat2","disease_duration_in_cat2","dmt_glucocorticoid","has_comorbidities","covid_wave"]
    #Feasability study
    df = df_in.copy()

    for v in main_vars+extra_vars+stratification_cols:
        df.loc[df[v].astype(str)=="",v] = None

    #Main Analysis
    if central_platform:
        df["source"] = df["secret_name"].apply(lambda x : x.split("_")[0])
        #df.loc[df.source=="C","source"]  = df.loc[df.source=="C","source"].str.cat(df.loc[df.source=="C","covid19_country"],sep="_")
        #df.loc[df.source=="P","source"]  = df.loc[df.source=="P","source"].str.cat(df.loc[df.source=="P","covid19_country"],sep="_")
        df.loc[df.covid19_country.isna(),"covid19_country"] = "missing"
        df["source"]  = df["source"].str.cat(df["covid19_country"],sep="_")
        #df["source"]  = df["source"].str.cat(df["covid19_country"],sep="_")
    else:
        df["source"] = None

    for extra_var in extra_vars:
        if extra_var in df:
            for strat_col in stratification_cols:
                if strat_col in df:
                    df.fillna("missing").groupby(["covid19_diagnosis","source"]+main_vars+[strat_col]).count()[["secret_name"]].reset_index().to_csv(f"./results/{outfile}{report_source}_{extra_var}_{strat_col}.csv")
                else:
                    print(f"Warning : {strat_col} not in data !")
        else:
            print(f"Warning : {main_var} not in data !")
