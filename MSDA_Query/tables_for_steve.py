import pandas as pd
import itertools

old_danish = False
corrected_danish = False
main_dir = "aggregation"
include_patients = False

def extract_country(x):
    l = x.split("_")
    if len(l)>1:
        return "_".join(l[1:])
    else:
        return l[0]

outcome_list = ["covid19_admission_hospital","covid19_icu_stay","covid19_outcome_death","covid19_ventilation","covid19_outcome_ventilation_or_ICU"]
variables_list = ["multi2_" + "_AND_".join(["sex_binary","edss_in_cat2","ms_type2"])]

interactions_cols = ["sex_binary","disease_duration_in_cat2","edss_in_cat2","ms_type2","bmi_in_cat2","has_comorbidities","current_smoker"]
variables_list += ["multi3_" + "_AND_".join(interactions) for interactions in itertools.combinations(interactions_cols,6)]

for outcome in outcome_list:
    for variables in  variables_list:
        if ('sex_binary' not in variables) or ("edss_in_cat2" not in variables) or("ms_type2" not in variables):
            continue
        if include_patients:
            df_p = pd.read_csv(f"./results/{main_dir}/agg_patients_{variables}_{outcome}.csv")
        df_c = pd.read_csv(f"./results/{main_dir}/agg_clinicians_{variables}_{outcome}.csv")

        if old_danish and "multi2" in variables:
            df_c_old = pd.read_csv(f"./results/old_cut/agg_clinicians_{variables}_{outcome}.csv")
            df_c.drop(df_c.loc[df_c.source.str.contains("COV19")].index,inplace = True)
            df_c = df_c.append(df_c_old.loc[df_c_old.source.str.contains("COV19")])
        elif corrected_danish:
            df_c_corrected = pd.read_csv(f"./results/aggregation/agg_clinicians_{variables}_{outcome}.csv")
            df_c.drop(df_c.loc[df_c.source.str.contains("COV19")].index,inplace = True)
            df_c = df_c.append(df_c_corrected.loc[df_c_corrected.source.str.contains("COV19")])

        if include_patients:
            print("dropping underaged patients")
            print(df_p.loc[df_p.age_in_cat=="0","agg_counts"].sum())
            df_p = df_p.loc[df_p.age_in_cat!="0"]
        print("dropping underaged patients")
        print(df_c.loc[df_c.age_in_cat=="0","agg_counts"].sum())
        df_c = df_c.loc[df_c.age_in_cat!="0"]

        #print(edward)

        if "covid19_country" not in df_c:

            if include_patients:
                df_p["covid19_country"] = df_p.source.apply(extract_country)
                df_p["source"] = df_p.source.apply(lambda x: x.split("_")[0])

            df_c["covid19_country"] = df_c.source.apply(extract_country)
            df_c["source"] = df_c.source.apply(lambda x: x.split("_")[0])

        if include_patients:
            df_p.to_csv(f"./results/Steve/agg_patients_{variables}_{outcome}.csv", index = False)

        if old_danish and "multi2" in variables:
            df_c.to_csv(f"./results/Steve/agg_clinicians_old_danish_{variables}_{outcome}.csv", index = False)
        elif corrected_danish:
            df_c.to_csv(f"./results/Steve/agg_clinicians_corrected_danish_{variables}_{outcome}.csv", index = False)
        else:
            df_c.to_csv(f"./results/Steve/agg_clinicians_{variables}_{outcome}.csv", index = False)
