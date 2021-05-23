import pandas as pd
import numpy as np
import datetime


def clean_dates(df_in):
    df = df_in.copy()
    df_types = df.dtypes
    date_cols = list(df_types.loc[df_types=="datetime64[ns]"].index) + ["covid19_admission_hospital_release","covid19_admission_hospital_date","covid19_date_lab_test",
                                                            "covid19_date_suspected_onset","dmt_start_date","edss_date_diagnosis","ms_diagnosis_date","ms_onset_date","covid19_outcome_death_date",
                                                                       'covid19_self_isolation_date','dmt_end_date','dmt_glucocorticoid_start_date',
       'dmt_glucocorticoid_stop_date','dmt_stop_date']
    for col in date_cols:
        df[col] = pd.to_datetime(df[col],errors="coerce")
    return df


def create_covid19_diagnosis(df_in):
    df = df_in.copy()
    df["covid19_diagnosis"] = "not_suspected"
    sympt_cols = [c for c in df.columns if "covid19_sympt" in c]

    for col in (["covid19_suspected_case","covid19_confirmed_case"]+sympt_cols):
        df[col] = df[col].astype(str).str.lower()
    #Patient reported_data
    df.loc[(df.report_source=="patients") &
     ((df.covid19_sympt_fever=="yes") | (df.covid19_sympt_dry_cough=="yes") | (df.covid19_sympt_loss_smell_taste=="yes")) &
     (df.covid19_suspected_case=="yes"),"covid19_diagnosis"] = "suspected"

    df.loc[ (df.report_source =="patients")
            & ((df.covid19_sympt_fever=="yes") | (df.covid19_sympt_dry_cough=="yes") | (df.covid19_sympt_loss_smell_taste=="yes"))
            & ((df.covid19_sympt_sore_throat=="yes") | (df.covid19_sympt_shortness_breath=="yes") | (df.covid19_sympt_pneumonia=="yes") | (df.covid19_sympt_fatigue=="yes") | (df.covid19_sympt_pain=="yes")|(df.covid19_sympt_nasal_congestion=="yes")|(df.covid19_sympt_chills=="yes")), "covid19_diagnosis"] = "suspected"

    #df.loc[(df.report_source=="patients") & (df.covid19_sympt_fever=="yes")&(df.covid19_suspected_case=="yes") & ((df.covid19_sympt_dry_cough=="yes") | (df.covid19_sympt_shortness_breath=="yes") | (df.covid19_sympt_pneumonia=="yes")),"covid19_diagnosis"] = "suspected"

    #df.loc[ (df.report_source =="patients")
    #        & (df.covid19_sympt_fever=="yes")
    #        & ((df.covid19_sympt_dry_cough=="yes") | (df.covid19_sympt_shortness_breath == "yes")| (df.covid19_sympt_pneumonia == "yes"))
    #        & (((df.covid19_sympt_fatigue=="yes") | (df.covid19_sympt_pain=="yes")) | (df.covid19_sympt_nasal_congestion=="yes") | (df.covid19_sympt_chills=="yes")|(df.covid19_sympt_loss_smell_taste=="yes")|(df.covid19_sympt_sore_throat=="yes")), "covid19_diagnosis"] = "suspected"

    df.loc[ (df.report_source == "patients") & (df.covid19_confirmed_case=="yes"),"covid19_diagnosis"] = "confirmed"

    #Clinicians reported data
    df.loc[(df.report_source.str.contains("clinician")) & (df.covid19_suspected_case=="yes"),"covid19_diagnosis"]= "suspected"
    df.loc[(df.report_source.str.contains("clinician")) & (df.covid19_confirmed_case=="yes"),"covid19_diagnosis"]= "confirmed"
    return df


def check_heights_weights(df_in):
    df = df_in.copy()
    assert df.loc[(df.height<100) | (df.height>210), "height"].shape[0]==0
    assert df.loc[(df.weight<30) | (df.weight>300), "weight"].shape[0]==0
    if (("height" in df) and ("weight" in df)):
        df.height = pd.to_numeric(df.height,errors = "coerce")
        df.weight = pd.to_numeric(df.weight, errors = "coerce")
        df.loc[(df.height<100) | (df.height>210), "height"] = np.nan
        df.loc[(df.weight<30) | (df.weight>300), "weight"] = np.nan
    else:
        print(f" Warning : height or weight not in data !")
    return
    #return df


def create_current_or_former_smoker(df_in):
    df = df_in.copy()
    if (("current_smoker" in df) and ("former_smoker" in df)):
        df["current_or_former_smoker"] = None
        df.current_smoker = df.current_smoker.str.lower()
        df.former_smoker = df.former_smoker.str.lower()
        df.loc[((df.current_smoker=="yes") | (df.former_smoker=="yes")), "current_or_former_smoker" ] =  "yes"
    else:
        print(f" Warning : current_smoker or former_smoker not in data !")
    return df

def create_bmi(df_in):
    df = df_in.copy()
    #Clean heights and weights AND CREATE BMI
    if (("weight" in df) and ("height" in df)):
        check_heights_weights(df)
        df["bmi"] = df.weight/ (df.height/100)**2
    else:
        print(f" Warning : weight or height not in data !")
    return df


def create_edss_in_cat(df_in):
    df = df_in.copy()
    # EDSS in cat
    if "edss_value" in df:
        df["edss_in_cat"] = None

        df['edss_value'] = pd.to_numeric(df['edss_value'],errors='coerce')
        df.loc[(df.edss_value>=0) & (df.edss_value<=3),"edss_in_cat"] = 0
        df.loc[(df.edss_value>3) & (df.edss_value<=6),"edss_in_cat"] = 1
        df.loc[(df.edss_value>6),"edss_in_cat"] = 2

        df["edss_in_cat2"] = None

        df.loc[(df.edss_value>=0) & (df.edss_value<=6),"edss_in_cat2"] = 0
        df.loc[(df.edss_value>6),"edss_in_cat2"] = 1
    else:
        print(f" Warning : edss_value not in data !")
    return df

def create_bmi_in_cat(df_in):
    df = df_in.copy()
    if "bmi" in df:
        df["bmi_in_cat"] = None
        df.loc[(df.bmi<18.5) & (df.bmi>0),"bmi_in_cat"] = "underweight"
        df.loc[(df.bmi<=25) & (df.bmi>=18.5),"bmi_in_cat"] = "normal"
        df.loc[(df.bmi<=30) & (df.bmi>25),"bmi_in_cat"] = "overweight"
        df.loc[(df.bmi<=35) & (df.bmi>30),"bmi_in_cat"] = "class I obesity"
        df.loc[ (df.bmi>35),"bmi_in_cat"] = "class II obesity"

        df["bmi_in_cat2"] = None
        df.loc[df.bmi<=30,"bmi_in_cat2"] = "not_overweight"
        df.loc[df.bmi>30,"bmi_in_cat2"]  = "overweight"
    return df

def create_age_in_cat(df_in):
    df = df_in.copy()
    if "age_years" in df:
        df["age_in_cat"] = None
        df.loc[(df.age_years<18) & (df.age_years>0),"age_in_cat"] = 0
        df.loc[(df.age_years<=50) & (df.age_years>=18),"age_in_cat"] = 1
        df.loc[(df.age_years<=70) & (df.age_years>50),"age_in_cat"] = 2
        df.loc[ (df.age_years>70),"age_in_cat"] = 3
    return df

def create_ms_onset_date(df_in):
    df = df_in.copy()
    if "ms_onset_date" in df:
        df["year_onset"] = pd.to_datetime(df.ms_onset_date, errors = 'coerce').dt.year
    return df

def create_ventilation_or_ICU(df_in):
    df = df_in.copy()
    if (("covid19_ventilation" in df) and ("covid19_icu_stay" in df)):
        df.covid19_ventilation = df.covid19_ventilation.str.lower()
        df.covid19_icu_stay = df.covid19_icu_stay.str.lower()

        df["covid19_outcome_ventilation_or_ICU"] = None
        df.loc[(df.covid19_ventilation=="yes"),"covid19_outcome_ventilation_or_ICU"] = "yes"
        df.loc[(df.covid19_icu_stay=="yes"),"covid19_outcome_ventilation_or_ICU"] = "yes"
        df.loc[((df.covid19_ventilation=="no") & (df.covid19_icu_stay=="no")),"covid19_outcome_ventilation_or_ICU"] = "no"
    return df


def create_sex_binary(df_in):
    df = df_in.copy()

    if 'sex' in df:
        df.sex = df.sex.str.lower()
        df["sex_binary"] = None
        df.loc[df.sex=="male","sex_binary"] = "male"
        df.loc[df.sex=="female","sex_binary"] = "female"
    return df

def create_covid19_date_reporting(df_in):
    df = df_in.copy()
    if "covid19_date_reporting" in df:
        df["year_reporting"] = df.covid19_date_reporting.dt.year
    return df

def create_type_dmt(df_in):
    df = df_in.copy()
    # type_last_dmt
    if (("dmt_stop_date" in df ) and ("current_dmt" in df) and ("covid19_date_reporting" in df)) :

        df["type_last_dmt"] = None
        df["type_current_dmt"] = None
        df.loc[(df.current_dmt=="yes") | ((df.current_dmt=="no") & ((pd.to_datetime(df.dmt_stop_date,errors="coerce") - pd.to_datetime(df.covid19_date_reporting,errors="coerce")).dt.days <180)),"type_last_dmt"] = df.loc[(df.current_dmt=="yes") | ((df.current_dmt=="no") & ((pd.to_datetime(df.dmt_stop_date,errors="coerce") - pd.to_datetime(df.covid19_date_reporting,errors="coerce")).dt.days <180)),"type_dmt"]

        df.loc[df.current_dmt=="yes","type_current_dmt"] = df.loc[df.current_dmt=="yes","type_dmt"]
    else:
        print(f"Warning : dmt_stop_date or current_dmt or covid19_date_reporting not in data !")
    return df

def create_dmt_overall(df_in):
    df = df_in.copy()

    if (("current_dmt" in df ) and ("type_dmt" in df )):
        df["dmt_type_overall"] = None
        df.current_dmt = df.current_dmt.str.lower()
        df.type_dmt = df.type_dmt.str.lower()
        df.loc[((df.current_dmt.isna()) & (df.type_dmt.isna())),"dmt_type_overall"] = "No Information on DMT use"
        df.loc[((df.current_dmt=="no")  | (df.current_dmt=="no, but was in the past")),"dmt_type_overall"] = "currently not using any DMT"
        df.loc[((df.current_dmt=="yes") & (df.type_dmt=="interferons")),"dmt_type_overall"] = "currently on interferon"
        df.loc[((df.current_dmt=="yes") & (df.type_dmt=="glatiramer")),"dmt_type_overall"] = "currently on glatiramer"
        df.loc[((df.current_dmt=="yes") & (df.type_dmt=="natalizumab")),"dmt_type_overall"] = "currently on natalizumab"
        df.loc[((df.current_dmt=="yes") & (df.type_dmt=="ocrelizumab")),"dmt_type_overall"] = "currently on ocrelizumab"
        df.loc[((df.current_dmt=="yes") & (df.type_dmt=="fingolimod")),"dmt_type_overall"] = "currently on fingolimod"
        df.loc[((df.current_dmt=="yes") & (df.type_dmt=="dimethyl_fumarate")),"dmt_type_overall"] = "currently on dimethyl fumarate"
        df.loc[((df.current_dmt=="yes") & (df.type_dmt=="teriflunomide")),"dmt_type_overall"] = "currently on teriflunomide"
        df.loc[((df.current_dmt=="yes") & (df.type_dmt=="alemtuzumab")),"dmt_type_overall"] = "currently on alemtuzumab"
        df.loc[((df.current_dmt=="yes") & (df.type_dmt=="cladribine")),"dmt_type_overall"] = "currently on cladribine"
        df.loc[((df.current_dmt=="yes") & (df.type_dmt=="siponimod")),"dmt_type_overall"] = "currently on siponimod"
        df.loc[((df.current_dmt=="yes") & (df.type_dmt=="rituximab")),"dmt_type_overall"] = "currently on rituximab"
        if ("type_dmt_other" in df):
            df.loc[((df.current_dmt=="yes") & (~ df.type_dmt_other.isna())),"dmt_type_overall"] = "currently on another drug not listed"
    else:
        print(f"Warning : current_dmt or type_dmt not in data !")

    return df


def create_disease_duration(df_in):
    df = df_in.copy()
    if (("year_reporting" in df) & ("year_onset" in df)):
        df["disease_duration"] = df.year_reporting - df.year_onset

        df["disease_duration_in_cat"] = None
        df.loc[(df.disease_duration<2) & (df.disease_duration>0),"disease_duration_in_cat"] = 0
        df.loc[(df.disease_duration<=10) & (df.disease_duration>=2),"disease_duration_in_cat"] = 1
        df.loc[(df.disease_duration>10),"disease_duration_in_cat"] = 2

        df["disease_duration_in_cat2"] = None
        df.loc[(df.disease_duration<=10) & (df.disease_duration>0),"disease_duration_in_cat2"] = 0
        df.loc[(df.disease_duration>10),"disease_duration_in_cat2"] = 1
    return df

def create_ms_type2(df_in):
    df = df_in.copy()
    if "ms_type" in df:
        df["ms_type2"] = None
        df.loc[df.ms_type=="RRMS","ms_type2"] = "relapsing_remitting"
        df.loc[((df.ms_type=="CIS") | (df.ms_type=="") | (df.ms_type=="not_sure")),"ms_type2"] = "other"
        df.loc[((df.ms_type=="SPMS") |(df.ms_type=="PPMS")),"ms_type2"] = "progressive_MS"
    return df

def create_covid19_outcome_levels_1(df_in):
    df = df_in.copy()

    df.covid19_ventilation = df.covid19_ventilation.str.lower()
    df.covid19_icu_stay = df.covid19_icu_stay.str.lower()
    df.covid19_admission_hospital = df.covid19_admission_hospital.str.lower()
    df.covid19_outcome_death = df.covid19_outcome_death.str.lower()

    df["covid19_outcome_levels_1"] = None
    df.loc[df.covid19_admission_hospital=="no","covid19_outcome_levels_1"] = 0
    df.loc[(df.covid19_admission_hospital=="yes") & ((df.covid19_icu_stay=="no") | (df.covid19_ventilation=="no") ),"covid19_outcome_levels_1"] = 1
    df.loc[(df.covid19_icu_stay=="yes") | (df.covid19_ventilation=="yes"),"covid19_outcome_levels_1"] = 2
    df.loc[df.covid19_outcome_death=="yes","covid19_outcome_levels_1"] = 3

    return df

def create_covid19_outcome_levels_2(df_in):
    df = df_in.copy()

    df.covid19_ventilation = df.covid19_ventilation.str.lower()
    df.covid19_icu_stay = df.covid19_icu_stay.str.lower()
    df.covid19_admission_hospital = df.covid19_admission_hospital.str.lower()
    df.covid19_outcome_death = df.covid19_outcome_death.str.lower()

    df["covid19_outcome_levels_2"] = None
    df.loc[df.covid19_admission_hospital=="no","covid19_outcome_levels_2"] = 0
    df.loc[(df.covid19_admission_hospital=="yes") ,"covid19_outcome_levels_2"] = 1
    df.loc[(df.covid19_icu_stay=="yes") | (df.covid19_ventilation=="yes"),"covid19_outcome_levels_2"] = 2
    df.loc[df.covid19_outcome_death=="yes","covid19_outcome_levels_2"] = 3

    return df

def create_duration_treatment_cat(df_in):
    df = df_in.copy()

    if ("covid19_date_suspected_onset" in df.columns) and ("dmt_start_date" in df.columns):
        df["duration_treatment"] = (df["covid19_date_suspected_onset"] - df["dmt_start_date"]).dt.days
    else:
        print("Warning : impossible to compute treatment duration !")
        df["duration_treatment"] = None

    df["duration_treatment_cat1"] = None
    df.loc[df["duration_treatment"]<365,"duration_treatment_cat1"] = 0
    df.loc[(df["duration_treatment"]>=365)&(df["duration_treatment"]<730),"duration_treatment_cat1"] = 1
    df.loc[df["duration_treatment"]>=730,"duration_treatment_cat1"] = 2

    df["duration_treatment_cat2"] = None
    df.loc[df["duration_treatment"]<182,"duration_treatment_cat2"] = 0
    df.loc[df["duration_treatment"]>=182,"duration_treatment_cat2"] = 1

    return df

def create_covid_wave(df_in):
    df = df_in.copy()

    if "covid_wave1" not in df.columns:
        df["covid_wave1"] = None

    if "covid_wave2" not in df.columns:
        df["covid_wave2"] = None
        if "covid19_date_suspected_onset" in df.columns:
            df.loc[df.covid19_date_suspected_onset < datetime.datetime(2020,5,31),"covid_wave2"] = 1
            df.loc[df.covid19_date_suspected_onset > datetime.datetime(2020,10,1),"covid_wave2"] = 2

    return df

def enhance_data(df_in):
    df = df_in.copy()
    df = create_covid19_diagnosis(df)

    df = create_bmi(df)
    df = create_ventilation_or_ICU(df)
    df = create_sex_binary(df)
    df = create_edss_in_cat(df)
    df = create_bmi_in_cat(df)
    df = create_age_in_cat(df)
    df = create_ms_onset_date(df)
    df = create_covid19_date_reporting(df)
    df = create_disease_duration(df)
    df = create_ms_type2(df)
    df = create_current_or_former_smoker(df)
    df = create_type_dmt(df)
    df = create_dmt_overall(df)

    df = create_covid19_outcome_levels_1(df)
    df = create_covid19_outcome_levels_2(df)

    df = create_duration_treatment_cat(df)

    df = create_covid_wave(df)

    return df

def enhance_forms_data(df_in):

    df = df_in.copy()
    df["Origin"] = "Forms"
    df["report_source"] = "clinicians"

    df.loc[df.secret_name.str.contains("C_"),"report_source"] = "clinicians"
    df.loc[df.secret_name.str.contains("P_"),"report_source"] = "patients"

    df = enhance_data(df)

    return df

def enhance_registry_data(df_in):

    df = df_in.copy()
    df["Origin"] = "Registry"
    df["report_source"] = "clinicians"
    df.loc[df.secret_name.str.contains("COV0_"),"report_source"] = "clinicians"
    df.loc[df.secret_name.str.contains("COV1_"),"report_source"] = "clinicians"
    df.loc[df.secret_name.str.contains("COV2C"),"report_source"] = "clinicians"
    df.loc[df.secret_name.str.contains("COV2P"),"report_source"] = "patients"
    df.loc[df.secret_name.str.contains("COV3C"),"report_source"] = "clinicians"
    df.loc[df.secret_name.str.contains("COV3P"),"report_source"] = "patients"
    df.loc[df.secret_name.str.contains("COV4"),"report_source"] = "clinicians"
    df.loc[df.secret_name.str.contains("COV5"),"report_source"] = "clinicians"
    df.loc[df.secret_name.str.contains("COV6"),"report_source"] = "clinicians"
    df.loc[df.secret_name.str.contains("COV7"),"report_source"] = "clinicians"
    df.loc[df.secret_name.str.contains("COV8C"),"report_source"] = "clinicians"
    df.loc[df.secret_name.str.contains("COV8P"),"report_source"] = "patients"
    df.loc[df.secret_name.str.contains("COV9"),"report_source"] = "clinicians"
    df.loc[df.secret_name.str.contains("COV10"),"report_source"] = "patients"
    df.loc[df.secret_name.str.contains("COV11"),"report_source"] = "patients"
    df.loc[df.secret_name.str.contains("COV12"),"report_source"] = "patients"
    df.loc[df.secret_name.str.contains("COV13"),"report_source"] = "clinicians"
    df.loc[df.secret_name.str.contains("COV14C"),"report_source"] = "clinicians" # AND patient"
    df.loc[df.secret_name.str.contains("COV14P"),"report_source"] = "patients" # AND patient"
    df.loc[df.secret_name.str.contains("COV15"),"report_source"] = "patients"
    df.loc[df.secret_name.str.contains("COV17"),"report_source"] = "patients" # AND patient"
    df.loc[df.secret_name.str.contains("COV17P"),"report_source"] = "patients" # AND patient"
    df.loc[df.secret_name.str.contains("COV17C"),"report_source"] = "clinicians" # AND patient"
    df.loc[df.secret_name.str.contains("COV18"),"report_source"] = "clinicians"
    df.loc[df.secret_name.str.contains("COV19"),"report_source"] = "clinicians"
    df.loc[df.secret_name.str.contains("COV20"),"report_source"] = "clinicians"
    df.loc[df.secret_name.str.contains("COV22"),"report_source"] = "patients"
    df.loc[df.secret_name.str.contains("COV29"),"report_source"] = "patients"

    df = enhance_data(df)

    return df
