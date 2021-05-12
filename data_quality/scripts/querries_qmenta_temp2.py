import pandas as pd
import numpy as np
#from pandas_profiling import ProfileReport
import os
import itertools


def compute_tables(df_in, report_source, outfile = "", report_function=None):

    os.makedirs("./results", exist_ok=True)

    stratification_cols = ["covid19_admission_hospital","covid19_icu_stay","covid19_ventilation","covid19_outcome_death","covid19_outcome_ventilation_or_ICU"]

    #Feasability study
    df = df_in.copy()

    df.groupby("covid19_diagnosis").count()["secret_name"].reset_index().to_csv(f"./results/{outfile}{report_source}_suspected_counts.csv")

    for main_var in stratification_cols:
        if main_var in df:
            df.loc[df[main_var]=="",main_var] = None
            df.groupby("covid19_diagnosis")[main_var].value_counts().to_csv(f"./results/{outfile}{report_source}_strat_{main_var}.csv",header = True)
        else:
            print(f" Warning : {main_var} not in the data !")
            
## QVreport_function is existing in the qmenta script only:
#    if report_function is not None:
#        report_function(df, f"./results/{outfile}{report_source}_profile.tex")
    if report_function is not None:
        report_function(df, f"./reports/{report_source}_enhanced_profile.tex")
        print(f"{report_source}_profile built!")
    else:
        print("Problem with File")
    #profile = ProfileReport(df, title="Pandas Profiling Report",minimal = True, samples = None)
    #profile2 = ProfileReport(df.loc[:,df.columns[int(len(df.columns)/2):]], title="Pandas Profiling Report B",minimal = True, samples = None)

    #profile.to_file(f"./results/{outfile}{report_source}_profile.html")
    #profile2.to_file(f"./results/{outfile}{report_source}_profile2.html")
    # As a json
    #profile.to_file(f"./results/{outfile}{report_source}_profile.json")
    #profile2.to_file(f"./results/{outfile}{report_source}_profile2.json")


    #Univariate Analysis

    for main_var in ["age_in_cat","bmi_in_cat","bmi_in_cat2","edss_in_cat","edss_in_cat2","ms_type2",
                    "disease_duration_in_cat","disease_duration_in_cat2","sex","sex_binary",
                    "ms_type","current_smoker","former_smoker","current_dmt","type_last_dmt",
                    "type_current_dmt","dmt_type_overall","dmt_glucocorticoid","has_comorbidities",
                    "com_cardiovascular_disease","com_diabetes","com_chronic_liver_disease",
                    "com_chronic_kidney_disease","com_neurological_neuromuscular","com_lung_disease",
                    "com_immunodeficiency","com_malignancy"]:
        if main_var in df:
            df.loc[df[main_var]=="",main_var] = None
            for strat_col in stratification_cols:
                if strat_col in df:
                    df.fillna("missing").groupby(["covid19_diagnosis",strat_col,main_var]).count()[["secret_name"]].reset_index().to_csv(f"./results/{outfile}{report_source}_{main_var}_{strat_col}.csv")
                else:
                    print(f"Warning : {strat_col} not in data !")
        else:
            print(f"Warning : {main_var} not in data !")

    #Multivariate Analysis
    if "dmt_type_overall" in df and "age_in_cat" in df:
        for main_var in ["sex_binary","current_or_former_smoker","bmi_in_cat2","ms_type2","disease_duration_in_cat2","edss_in_cat2",
                        "dmt_glucocorticoid","has_comorbidities","com_cardiovascular_disease", "com_diabetes",
                        "com_chronic_liver_disease","com_neurological_neuromuscular","com_lung_disease",
                        "com_immunodeficiency","com_malignancy"]:
            if main_var in df:
                for strat_col in stratification_cols:
                    if strat_col in df:
                        df.fillna("missing").groupby(["covid19_diagnosis","dmt_type_overall","age_in_cat",strat_col,main_var]).count()[["secret_name"]].reset_index().to_csv(f"./results/{outfile}{report_source}_multi_{main_var}_{strat_col}.csv")
                    else:
                        print(f"Warning : {strat_col} not in data !")
            else:
                print(f"Warning : {main_var} not in data !")

        interactions_cols = ["sex_binary","disease_duration_in_cat2","edss_in_cat2","ms_type2","dmt_glucocorticoid","has_comorbidities"]

        for interactions in itertools.combinations(interactions_cols,3):
            interactions = list(interactions)
            if sum([var in df for var in interactions])==len(interactions):
                for strat_col in stratification_cols:
                    if strat_col in df:
                        fname = "_AND_".join(interactions)
                        df.fillna("missing").groupby(["covid19_diagnosis","dmt_type_overall","age_in_cat",strat_col]+interactions).count()[["secret_name"]].reset_index().to_csv(f"./results/{outfile}{report_source}_multi2_{fname}_{strat_col}.csv")
                    else:
                        print(f"Warning : {strat_col} not in data !")