import pandas as pd
from os import listdir

source = ["clinicians","patients"]

df_total_list = []


def extract_country(x):
    l = x.split("_")
    if len(l)>1:
        return "_".join(l[1:])
    else:
        return l[0]

for s in source:

    f_list = [f.split("_covid19_icu")[0] for f in listdir("./results/aggregation/") if (("multi2" in f) and (s in f) and ("icu_stay") in f) ]


    outcome_list = ["covid19_admission_hospital","covid19_icu_stay","covid19_outcome_death","covid19_ventilation","covid19_outcome_ventilation_or_ICU"]
    df_main_list = []

    for f in f_list:
        df_list = []
        for outcome in outcome_list:
            df = pd.read_csv(f"./results/aggregation/{f}_{outcome}.csv")
            df["outcome"] = outcome
            df["outcome_value"] = df[outcome]
            df.drop(columns = [outcome],inplace = True)
            df_list += [df]
        df_ = pd.concat(df_list)

        confounders = f.split("multi2_")[1].split("_AND_")
        for i,c in enumerate(confounders):
            df_[f"confounder_{i+1}"] = df[c]
            df_[f"confounder_{i+1}_type"] = c


        df_main_list += [df_[["covid19_diagnosis","confounder_1", "confounder_1_type","confounder_2", "confounder_2_type",
                             "confounder_3", "confounder_3_type", "dmt_type_overall","age_in_cat","source","agg_counts",
                              "outcome","outcome_value"]]]

    df_full = pd.concat(df_main_list)

    df_full["covid19_country"] = df_full.source.apply(extract_country)
    df_full["source"] = df_full.source.apply(lambda x: x.split("_")[0])

    df_full.to_csv(f"./results/viztool/multi_data_{s}.csv",index = False)

    if s == "clinicians":
        df_full["reporting_source"] = "C"
    elif s=="patients":
        df_full["reporting_source"] = "P"
    else:
        df_full["reporting_source"] = None

    #new names for the drugs. "No information on DMT use" is casted as missing.
    df_full["dmt_type_overall"] = df_full.dmt_type_overall.apply(lambda x : x.split(" ")[-1]).replace({"use":"missing","listed":"other","DMT":"no_dmt"})


    df_total_list +=[df_full]

df_total = pd.concat(df_total_list)
df_total.insert(0,"secret_name",[f"D_{i}/1" for i in range(df_total.shape[0])])
cols = df_total.columns.tolist()
cols = [cols[0]] + [cols[-1]] + cols[1:-1]
df_total = df_total[cols]
df_total["agg_counts"] = df_total["agg_counts"].astype(int)

df_total.to_csv(f"./results/viztool/multi_data_total.csv",index = False)
