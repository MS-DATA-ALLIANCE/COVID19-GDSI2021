import pandas as pd
from os import listdir, remove
from os.path import isfile, join
import numpy as np

def aggregate_tables(directories = ["Germany","QMENTA"]):
    dir_dict = {}
    files_names = []
    for dir_name in directories:
        mypath = f"./results/{dir_name}"
        files_in_dir = [f for f in listdir(mypath) if (isfile(join(mypath, f)) and (f[-4:]==".csv") and ("covid19" in f) and ("reference" not in f) and ("strat" not in f))]
        files_names += files_in_dir
        dir_dict[dir_name] = files_in_dir
    files_names = np.unique(files_names)

    for f in files_names:
        #print(f)
        df_list = []
        outcome_type = "covid19_" + f.split("covid19_")[-1].split(".")[0]
        col_of_interest = ["_".join(f.split("covid19")[0].split("_")[1:-1])]
        if "multi_" in f:
            col_of_interest = ["_".join(f.split("covid19")[0].split("_")[2:-1])]
        elif "multi2_" in f:
            col_of_interest = ("_".join(f.split("covid19")[0].split("_")[2:-1])).split("_AND_")
        elif "multi3_" in f:
            col_of_interest = ("_".join(f.split("covid19")[0].split("_")[2:-1])).split("_AND_")
        #mapping_dict = {"age":"Age_in_cat", "disease_duration": "disease_duration_in_cat", "edss":"edss_in_cat", "bmi" : "bmi_in_cat"}
        #if col_of_interest in mapping_dict:
        #        col_of_interest = mapping_dict[col_of_interest]

        for dir_name in directories:
            if f in dir_dict[dir_name]:
                df_temp = pd.read_csv(f"./results/{dir_name}/{f}")
                if (("multi2_" in f) or ("multi3_" in f)) and (not("QMENTA" in dir_name)):
                    df_temp["source"] = dir_name
                df_list += [df_temp]
            else:
                source_type = f.split("_")[0]
                extension = "_".join(f.split("_")[-3:])
                #type_file = "_".join(f.split("_")[-2:]).split(".")[0]
                type_file = f.split("covid19_")[-1].split(".")[0]

                if "covid19" in type_file:
                    type_file = type_file.split("_")[-1]

                reference_file_name = f"reference_{source_type}_{type_file}.csv"

                if reference_file_name in listdir(f"./results/{dir_name}"):
                    df_temp = pd.read_csv(f"./results/{dir_name}/{reference_file_name}")
                    df_temp[col_of_interest] = pd.DataFrame([["missing"]*len(col_of_interest)], index = df_temp.index)
                    if "multi" in f:
                        df_temp["dmt_type_overall"] = "missing"
                        df_temp["age_in_cat"] = "missing"
                    #if ("multi2_" in f) and ( not ("QMENTA" in dir_name)):
                    #    df_temp["source"] = dir_name
                    if ("multi2_" in f and ("source" not in df_temp.columns)):
                        df_temp["source"] = dir_name
                    elif ("multi3_" in f and ("source" not in df_temp.columns)):
                        df_temp["source"] = dir_name
                    df_list += [df_temp]
                else:
                    print(f"Warning : reference file is not present : {reference_file_name} - {dir_name}")


        merge_c = [c for c in df_list[0].columns if ("covid19" in c)] + col_of_interest
        if "multi" in f:
            merge_c += ["dmt_type_overall","age_in_cat"]
        if "multi2_" in f:
            merge_c +=["source"]
        if "multi3_" in f:
            merge_c +=["source"]

        for i,df_i in enumerate(df_list):
            df_i[outcome_type] = df_i[outcome_type].apply(lambda x: x.lower())
            df_i.rename(columns = {"secret_name":"count"}, inplace = True)
            if ("source" in merge_c) and ("source" not in df_i.columns):
                import ipdb; ipdb.set_trace()
            df_i[merge_c] = df_i[merge_c].astype(object)
            df_i[col_of_interest] = df_i[col_of_interest].apply(lambda x: x.astype(str).str.lower())
            if i == 0:
                df_m = df_i.copy()
            else:
                df_m = df_m.merge(df_i,on =  merge_c, how = "outer")

            agg_c = [c for c in df_m.columns if (("secret_name" in c) | ("count" in c))]
            df_m["agg_counts"] = df_m[agg_c].sum(axis=1)
            df_m = df_m[merge_c+ ["agg_counts"]]

        df_m[merge_c+ ["agg_counts"]].to_csv(f"./results/aggregation/agg_{f}", index = False)


def gen_tables_viz():
    mypath = "./results/aggregation/"
    files_in_dir = [f for f in listdir(mypath) if (isfile(join(mypath, f)) and (f[-4:]==".csv") and ("multi_" in f)) ]
    file_types = np.unique(["_".join(f.split("covid19")[0].split("_")[3:])[:-1] for f in files_in_dir]).tolist()

    df_all_list = []
    for type_f in file_types:
        df_temp_list = []
        for f in files_in_dir:
            if type_f in f:
                source_type = f.split("covid19")[0].split("_")[1]
                df_temp = pd.read_csv(join(mypath,f))
                covid_type = f.split("covid19")[1][1:-4]
                col_name = "covid19_"+covid_type
                df_temp.rename(columns = { col_name: "outcome_value"}, inplace = True)
                df_temp["outcome_type"] = covid_type
                df_temp["report_source"] = source_type
                df_temp_list += [df_temp]
        df_agg = pd.concat(df_temp_list).copy()
        df_agg.to_csv(f"./results/viztool/{type_f}.csv", index = False)

        df_agg["factor_field"] = type_f
        df_agg["factor_value"] = df_agg[type_f]
        df_agg.drop(columns=[type_f],inplace = True)
        df_all_list += [df_agg]
        #print(df_all_list[0].factor_field)

    df_all = pd.concat(df_all_list)
    #df_all["secret_name"] = np.arange(df_all.shape[0])
    df_all["dmt_type_overall"]=df_all.dmt_type_overall.str.split(" ").apply(lambda x: x[-1])
    df_all.loc[df_all.dmt_type_overall=="DMT","dmt_type_overall"] = "no_dmt"
    df_all.loc[df_all.dmt_type_overall=="use","dmt_type_overall"] = "missing"
    df_all.loc[df_all.dmt_type_overall=="listed","dmt_type_overall"] = "other"

    df_all["agg_counts"] = df_all["agg_counts"].astype(int)
    df_all.insert(0,"secret_name",[f"D_{i}/1" for i in range(df_all.shape[0])])

    df_all.to_csv(f"./results/viztool/global.csv",index = False)





def report_table_gen(report_source, feature, strats):

    df_out = pd.DataFrame(columns = ["Factor","Diagnosis","General","Hosp","Ventilation","ICU","Death","ICU_or_Ventilation"])

    df_list = [pd.read_csv(f"./results/aggregation/agg_{report_source}_{feature}_{strat}.csv") for strat in strats]

    N_suspected = df_list[0].loc[df_list[0].covid19_diagnosis=="suspected","agg_counts"].sum()
    N_confirmed = df_list[0].loc[df_list[0].covid19_diagnosis=="confirmed","agg_counts"].sum()
    N_sus_conf = N_suspected + N_confirmed


    df_out = df_out.append({"Factor" : "All", "Diagnosis" : "Suspected + Confirmed", "General" : N_sus_conf}, ignore_index= True)
    df_out = df_out.append({"Factor" : "All", "Diagnosis" : "Suspected", "General" : N_suspected}, ignore_index= True)
    df_out = df_out.append({"Factor" : "All", "Diagnosis" : "Suspected over both", "General" : N_suspected}, ignore_index= True)
    df_out = df_out.append({"Factor" : "All", "Diagnosis" : "Confirmed", "General" : N_confirmed}, ignore_index= True)
    df_out = df_out.append({"Factor" : "All", "Diagnosis" : "Confirmed over both", "General" : N_confirmed}, ignore_index= True)


    N_strat_sus_conf = [df_list[i].loc[((df_list[i][strat]=="yes") & ((df_list[i].covid19_diagnosis=="suspected") | (df_list[i].covid19_diagnosis=="confirmed"))),"agg_counts"].sum()/N_sus_conf if N_sus_conf!=0 else 0 for i, strat in enumerate(strats) ]

    N_strat_suspected = [df_list[i].loc[((df_list[i][strat]=="yes") & (df_list[i].covid19_diagnosis=="suspected")),"agg_counts"].sum()/N_suspected if N_suspected!=0 else 0 for i, strat in enumerate(strats) ]
    N_strat_suspected_bis = [df_list[i].loc[((df_list[i][strat]=="yes") & (df_list[i].covid19_diagnosis=="suspected")),"agg_counts"].sum()/N_sus_conf if N_sus_conf!=0 else 0 for i, strat in enumerate(strats) ]

    N_strat_confirmed = [df_list[i].loc[((df_list[i][strat]=="yes") & (df_list[i].covid19_diagnosis=="confirmed")),"agg_counts"].sum()/N_confirmed if N_confirmed!=0 else 0 for i, strat in enumerate(strats) ]
    N_strat_confirmed_bis = [df_list[i].loc[((df_list[i][strat]=="yes") & (df_list[i].covid19_diagnosis=="confirmed")),"agg_counts"].sum()/N_sus_conf if N_sus_conf!=0 else 0 for i, strat in enumerate(strats) ]

    df_out.loc[((df_out.Factor=="All") & (df_out.Diagnosis=="Suspected + Confirmed")),["Hosp","Ventilation","ICU","Death","ICU_or_Ventilation"]] = N_strat_sus_conf
    df_out.loc[((df_out.Factor=="All") & (df_out.Diagnosis=="Suspected")),["Hosp","Ventilation","ICU","Death","ICU_or_Ventilation"]] = N_strat_suspected
    df_out.loc[((df_out.Factor=="All") & (df_out.Diagnosis=="Suspected over both")),["Hosp","Ventilation","ICU","Death","ICU_or_Ventilation"]] = N_strat_suspected_bis

    df_out.loc[((df_out.Factor=="All") & (df_out.Diagnosis=="Confirmed")),["Hosp","Ventilation","ICU","Death","ICU_or_Ventilation"]] = N_strat_confirmed
    df_out.loc[((df_out.Factor=="All") & (df_out.Diagnosis=="Confirmed over both")),["Hosp","Ventilation","ICU","Death","ICU_or_Ventilation"]] = N_strat_confirmed_bis


    #For each factor :
    unique_facts = df_list[0][feature].unique()
    for feat_strat in unique_facts:
        N_suspected = df_list[0].loc[(df_list[0].covid19_diagnosis=="suspected")&(df_list[0][feature]==feat_strat),"agg_counts"].sum()
        N_confirmed = df_list[0].loc[(df_list[0].covid19_diagnosis=="confirmed")&(df_list[0][feature]==feat_strat),"agg_counts"].sum()
        N_sus_conf = N_suspected + N_confirmed

        df_out = df_out.append({"Factor" : feat_strat, "Diagnosis" : "Suspected + Confirmed", "General" : N_sus_conf}, ignore_index= True)
        df_out = df_out.append({"Factor" : feat_strat, "Diagnosis" : "Suspected", "General" : N_suspected}, ignore_index= True)
        df_out = df_out.append({"Factor" : feat_strat, "Diagnosis" : "Suspected over both", "General" : N_suspected}, ignore_index= True)
        df_out = df_out.append({"Factor" : feat_strat, "Diagnosis" : "Confirmed", "General" : N_confirmed}, ignore_index= True)
        df_out = df_out.append({"Factor" : feat_strat, "Diagnosis" : "Confirmed over both", "General" : N_confirmed}, ignore_index= True)


        N_strat_sus_conf = [df_list[i].loc[((df_list[i][strat]=="yes") & (df_list[i][feature]==feat_strat) & ((df_list[i].covid19_diagnosis=="suspected") | (df_list[i].covid19_diagnosis=="confirmed"))),"agg_counts"].sum()/N_sus_conf if N_sus_conf!=0 else 0 for i, strat in enumerate(strats) ]
        N_strat_suspected = [df_list[i].loc[((df_list[i][strat]=="yes") & (df_list[i][feature]==feat_strat) & (df_list[i].covid19_diagnosis=="suspected")),"agg_counts"].sum()/N_suspected if N_suspected!=0 else 0 for i, strat in enumerate(strats) ]
        N_strat_suspected_bis = [df_list[i].loc[((df_list[i][strat]=="yes") & (df_list[i][feature]==feat_strat) & (df_list[i].covid19_diagnosis=="suspected")),"agg_counts"].sum()/N_sus_conf if N_sus_conf!=0 else 0  for i, strat in enumerate(strats) ]

        N_strat_confirmed = [df_list[i].loc[((df_list[i][strat]=="yes") & (df_list[i][feature]==feat_strat) & (df_list[i].covid19_diagnosis=="confirmed")),"agg_counts"].sum()/N_confirmed if N_confirmed!=0 else 0 for i, strat in enumerate(strats) ]
        N_strat_confirmed_bis = [df_list[i].loc[((df_list[i][strat]=="yes") & (df_list[i][feature]==feat_strat) & (df_list[i].covid19_diagnosis=="confirmed")),"agg_counts"].sum()/N_sus_conf if N_sus_conf!=0 else 0 for i, strat in enumerate(strats) ]

        df_out.loc[((df_out.Factor==feat_strat) & (df_out.Diagnosis=="Suspected + Confirmed")),["Hosp","Ventilation","ICU","Death","ICU_or_Ventilation"]] = N_strat_sus_conf
        df_out.loc[((df_out.Factor==feat_strat) & (df_out.Diagnosis=="Suspected")),["Hosp","Ventilation","ICU","Death","ICU_or_Ventilation"]] = N_strat_suspected
        df_out.loc[((df_out.Factor==feat_strat) & (df_out.Diagnosis=="Suspected over both")),["Hosp","Ventilation","ICU","Death","ICU_or_Ventilation"]] = N_strat_suspected_bis

        df_out.loc[((df_out.Factor==feat_strat) & (df_out.Diagnosis=="Confirmed")),["Hosp","Ventilation","ICU","Death","ICU_or_Ventilation"]] = N_strat_confirmed
        df_out.loc[((df_out.Factor==feat_strat) & (df_out.Diagnosis=="Confirmed over both")),["Hosp","Ventilation","ICU","Death","ICU_or_Ventilation"]] = N_strat_confirmed_bis


    df_out.fillna(0,inplace = True)

    return(df_out)

def germany_fix(german_name):
    "Fixing outcome death in germany"
    if "outcome_death" in [f.split("covi19_")[-1].split(".")[0] for f in listdir(f"./results/{german_name}")]:
        print("There is")
    else:
        for source in ["patients","clinicians"]:
            print(f"There is no outcome death recorded in the German dataset. Creating one for {source}...")
            df = pd.read_csv(f"./results/{german_name}/{source}_age_in_cat_covid19_icu_stay.csv")
            df_ = df.groupby(["covid19_diagnosis","age_in_cat"])["secret_name"].sum().reset_index()
            df_["covid19_outcome_death"] = "missing"
            df_.to_csv(f"./results/{german_name}/{source}_age_in_cat_covid19_outcome_death.csv")

def uk_fix(uk_name):
    for f in listdir(f"./results/{uk_name}"):
        if "multi2_" in f:
            int1 = f.split("_AND_")[0].split("multi2_")[-1]
            int2 = f.split("_AND_")[1]
            int3 = f.split("_AND_")[2].split("_covid19_")[0]
            outcome = f.split("covid19_")[-1].split(".")[0]
            if f not in listdir(f"./results/QMENTA"):
                to_remove = True
                for f_q in listdir(f"./results/QMENTA"):
                    if (int1 in f_q) and (int2 in f_q) and (int3 in f_q) and (outcome in f_q):
                        df = pd.read_csv(f"./results/{uk_name}/{f}")
                        df.to_csv(f"./results/{uk_name}/{f_q}")
                        #print(f"Changing {f} for {f_q}")
                        remove(f"./results/{uk_name}/{f}")
                        to_remove = False
                if to_remove:
                    # File not needed
                    remove(f"./results/{uk_name}/{f}")


def compute_references(directories):
    dir_reference_dict = {}
    files_names = []
    for dir_name in directories:
        dir_reference_dict[dir_name] = {"patients" :{}, "clinicians" : {}}
        mypath = f"./results/{dir_name}"
        for f in listdir(mypath):
            if (isfile(join(mypath, f)) and (f[-4:]==".csv") and ("covid19" in f) and ("strat" not in f)):
                #type_file = "_".join(f.split("_")[-2:]).split(".")[0]
                type_file = f.split("covid19_")[-1].split(".")[0]
                #if "covid19" in type_file:
                #    type_file = type_file.split("_")[-1]
                if ("patients" in f) and (type_file not in dir_reference_dict[dir_name]["patients"]):
                    dir_reference_dict[dir_name]["patients"][type_file] = f

                elif ("clinicians" in f) and (type_file not in dir_reference_dict[dir_name]["clinicians"]):
                    dir_reference_dict[dir_name]["clinicians"][type_file] = f
                #else:
                    #print(f"Suspicious file : {f}")

    for repo in dir_reference_dict:
        for report_source in dir_reference_dict[repo]:
            #print(report_source)
            #for file_type in dir_reference_dict[repo][report_source]:
            for file_type in dir_reference_dict[repo][report_source]:
                f = dir_reference_dict[repo][report_source][file_type]
                #print(f"./results/{repo}/{f}")
                df_temp = pd.read_csv(f"./results/{repo}/{f}")
                df_temp.rename(columns = {"secret_name":"count"},inplace = True)
                df_temp = df_temp.groupby(["covid19_diagnosis",f"covid19_{file_type}"])["count"].sum().reset_index()
                df_temp.to_csv(f"./results/{repo}/reference_{report_source}_{file_type}.csv")


def feasibility_counts(feature, source,diagnosis):
    df_ = pd.read_csv(f"./results/reports/report_{source}_{feature}.csv")

    all = df_.loc[(df_.Factor=="All") & (df_.Diagnosis == diagnosis)].iloc[0]["General"]
    missing = df_.loc[(df_.Factor=="missing") & (df_.Diagnosis == diagnosis)].iloc[0]["General"]
    non_missing = all-missing

    return all, missing, non_missing

def iwMIS_fix():
    mypath = "./results/USA"
    dict_dmt = {"1":"alemtuzumab", "2":"missing", "3":"cladribine","4":"dimethyl_fumarate",
    "5" : "missing", "6": "missing", "7": "fingolimod", "8":"glatiramer", "9": "missing","10": "interferons", "11" : "missing",
    "12": "missing", "13":"missing", "14":"missing", "15":"natalizumab", "16":"ocrelizumab", "17" : "missing", "18":"rituximab",
    "19": "missing", "20":"siponimod", "21": "teriflunomide", "22":"missing",  "missing":"missing"}
    for f in listdir(mypath):
        if (isfile(join(mypath, f)) and ("dmt" in f) and ("type" in f)):
            col_of_interest = "_".join(f.split("covid19")[0].split("_")[1:-1])
            df_temp = pd.read_csv(join(mypath, f))
            df_temp[col_of_interest] = df_temp[col_of_interest].map(dict_dmt)

            df_temp.to_csv(join(mypath, f), index = False)
