import pandas as pd
import numpy as np
import datetime


def check_date_valid_covid19(column):
    """
    Checks if the year is below 2019 or date is above now()
    :param df:
    :return: a boolean vector, with true if the date is wrong, False otherwise
    """
    column = pd.to_datetime(column.copy())
    fail_ids_too_early = column.apply(lambda d: d.year < 2019)
    fail_ids_too_late = column > pd.Timestamp.now()

    return fail_ids_too_early | fail_ids_too_late

def check_date_valid_birth_now(column, age_col, reporting_date_column):
    """
    Checks if the year is below 2019 or date is above now()
    :param df:
    :return: a boolean vector, with true if the date is wrong, False otherwise
    """
    fail_ids_too_early = column.apply(lambda d: d.year) < reporting_date_column.apply(lambda d: d.year) - age_col
    fail_ids_too_late = column > reporting_date_column

    return fail_ids_too_early | fail_ids_too_late

def check_all_dates(df_in):
    used_columns = ["covid19_date_reporting", "age_years"]
    for col in used_columns:
        if col not in df_in.columns:
            print("Unable to clean", col, "as the column is not in the dataframe")
            return {}
    date_columns = [col for col in df_in.columns if "date" in col and "datetime" in str(df_in[col].dtype)]
    reporting_date_col = df_in["covid19_date_reporting"]
    age_col = df_in["age_years"]

    failures = {}

    for col in date_columns:
        failed_ids = df_in[check_date_valid_birth_now(df_in[col], age_col, reporting_date_col)]["id"].astype(int).tolist()
        for fail_id in failed_ids:
            if fail_id not in failures:
                failures[fail_id] = []
            failures[fail_id].append(col)

    return failures


def clean_covid19_date_reporting(df_in):
    if "covid19_date_reporting" not in df_in.columns:
        print("Unable to clean covid19_date_reporting as the column is not in the dataframe")
        return {}

    failures = {}

    fail_ids = df_in[df_in["covid19_date_reporting"].apply(lambda d: d.year<2019)]["id"].astype(int).tolist()

    for fail_id in fail_ids:
        failures[fail_id] = "covid19_date_reporting"

    return failures

def clean_year_reporting(df_in):
    if "year_reporting" not in df_in.columns:
        print("Unable to clean year_reporting as the column is not in the dataframe")
        return {}

    failures = {}

    fail_ids = df_in[df_in["year_reporting"]<2019]["id"].astype(int).tolist()

    for fail_id in fail_ids:
        failures[fail_id] = "year_reporting"

    return failures

def clean_covid19_has_symptoms(df_in):
    if "covid19_has_symptoms" not in df_in.columns:
        print("Unable to clean covid19_has_symptoms as the column is not in the dataframe")
        return {}

    failures = {}

    sympt_columns = [col for col in df_in.columns if col.startswith("covid19_sympt")]

    # Detect nan variables on all symptoms
    # df_na = df_in[df_in["covid19_has_symptoms"].isna()]
    # all_sympt_nan = df_na[sympt_columns].apply(lambda c: c.astype(str).str.lower()).apply(lambda r: np.all(r.isna()),axis=1) == True
    #
    # fail_ids = df_na[all_sympt_nan]["id"].astype(int).tolist()
    # for fail_id in fail_ids:
    #     failures[fail_id] = ["covid19_has_symptoms"] + sympt_columns

    # Detect sympt=no when at least one sympt variable is yes
    df_no = df_in[df_in["covid19_has_symptoms"].astype(str).str.lower() == "no"]
    any_sympt_yes = df_no[sympt_columns].apply(lambda c: c.astype(str).str.lower()).apply(lambda r: np.any(r == "yes"),axis=1) == True
    fail_ids_no = df_no[any_sympt_yes]["id"].astype(int).tolist()

    # df_yes = df_in[df_in["covid19_has_symptoms"].astype(str).str.lower() == "yes"]
    # no_sympt_yes = df_yes[sympt_columns].apply(lambda c: c.astype(str).str.lower()).apply(lambda r: np.any(r == "yes"),axis=1) == False
    # fail_ids_yes = df_yes[no_sympt_yes]["id"].astype(int).tolist()

    fail_ids = fail_ids_no # + fail_ids_yes

    for fail_id in fail_ids:
        failures[fail_id] = "covid19_has_symptoms"

    return failures

def repair_covid19_has_symptoms(df_in):
    if "covid19_has_symptoms" not in df_in.columns:
        print("Unable to repair covid19_has_symptoms as the column is not in the dataframe")
        return df_in

    df_out = df_in.copy()
    sympt_columns = [col for col in df_in.columns if col.startswith("covid19_sympt")]

    df_out.loc[df_out["covid19_has_symptoms"].isna() & df_out[sympt_columns].apply(lambda c: c.astype(str).str.lower()).apply(lambda r: np.any(r == "yes"),axis=1) == True, "covid19_has_symptoms"] = "yes"


    return df_out

def repair_type_dmt_other(df_in):
    if "type_dmt_other" not in df_in.columns:
        print("Unable to repair type_dmt_other as the column is not in the dataframe")
        return df_in
    df_out = df_in.copy()
    df_out.loc[df_out["type_dmt_other"]=="","type_dmt_other"] = None
    return df_out


def clean_covid19_self_isolation_date(df_in):
    if "covid19_self_isolation_date" not in df_in.columns:
        print("Unable to clean covid19_self_isolation_date as the column is not in the dataframe")
        return {}

    failures = {}

    fail_ids = df_in[check_date_valid_covid19(df_in["covid19_self_isolation_date"])]["id"].astype(int).tolist()

    for fail_id in fail_ids:
        failures[fail_id] = "covid19_self_isolation_date"

    return failures

def clean_covid19_self_isolation_duration(df_in):
    used_columns = ["covid19_self_isolation_duration", "covid19_date_reporting"]
    for col in used_columns:
        if col not in df_in.columns:
            print("Unable to clean", col, "as the column is not in the dataframe")
            return {}

    failures = {}
    fail_ids = df_in[pd.to_numeric(df_in["covid19_self_isolation_duration"],errors = "coerce") > (pd.to_datetime(df_in["covid19_date_reporting"],errors = "coerce")-pd.to_datetime(df_in["covid19_self_isolation_date"],errors = "coerce")).apply(lambda d: d.days)]["id"].astype(int).tolist()

    for fail_id in fail_ids:
        failures[fail_id] = "covid19_self_isolation_duration"

    return failures

def clean_covid19_date_lab_test(df_in):
    if "covid19_date_lab_test" not in df_in.columns:
        print("Unable to clean covid19_date_lab_test as the column is not in the dataframe")
        return {}

    failures = {}

    fail_ids = df_in[check_date_valid_covid19(df_in["covid19_date_lab_test"])]["id"].astype(int).tolist()

    for fail_id in fail_ids:
        failures[fail_id] = "covid19_date_lab_test"

    return failures

def clean_covid19_date_suspected_onset(df_in):
    if "covid19_date_suspected_onset" not in df_in.columns:
        print("Unable to clean covid19_date_suspected_onset as the column is not in the dataframe")
        return {}

    failures = {}

    fail_ids = df_in[check_date_valid_covid19(df_in["covid19_date_suspected_onset"])]["id"].astype(int).tolist()

    for fail_id in fail_ids:
        failures[fail_id] = "covid19_date_suspected_onset"

    return failures

def clean_covid19_admission_hospital_release(df_in):
    used_columns = ["covid19_admission_hospital_release", "covid19_admission_hospital_date"]
    for col in used_columns:
        if col not in df_in.columns:
            print("Unable to clean", col, "as the column is not in the dataframe")
            return {}

    failures = {}

    fail_ids = df_in[pd.to_datetime(df_in["covid19_admission_hospital_release"]) < pd.to_datetime(df_in["covid19_admission_hospital_date"])]["id"].astype(int).tolist()

    for fail_id in fail_ids:
        failures[fail_id] = "covid19_admission_hospital_release"

    return failures

def clean_covid19_icu_stay(df_in):
    used_columns = ["covid19_icu_stay", "covid19_admission_hospital"]
    for col in used_columns:
        if col not in df_in.columns:
            print("Unable to clean", col, "as the column is not in the dataframe")
            return {}

    failures = {}

    fail_ids = df_in[(df_in["covid19_icu_stay"].astype(str).str.lower() == "yes") & (df_in["covid19_admission_hospital"].astype(str).str.lower() == "no")]["id"].astype(int).tolist()

    for fail_id in fail_ids:
        failures[fail_id] = "covid19_icu_stay"

    return failures

def clean_covid19_ventilation(df):
    used_columns = ["covid19_ventilation", "covid19_ventilation_invasive", "covid19_ventilation_non_invasive"]
    for col in used_columns:
        if col not in df.columns:
            print("Unable to clean", col ,"as the column is not in the dataframe")
            return {}

    failures = {}

    df_in = df.copy()
    df_in["covid19_ventilation_invasive"] = df_in["covid19_ventilation_invasive"].astype(str)
    df_in["covid19_ventilation_non_invasive"] = df_in["covid19_ventilation_non_invasive"].astype(str)

    df_na = df_in[df_in["covid19_ventilation"].isna()]
    failed_ids_null = df_na[(df_na["covid19_ventilation_invasive"].astype(str).str.lower() == "yes") | (df_na["covid19_ventilation_non_invasive"].astype(str).str.lower() == "yes")]["id"].astype(int).tolist()

    df_no = df_in[df_in["covid19_ventilation"].astype(str).str.lower() == "no"]
    failed_ids_no = df_no[(df_no["covid19_ventilation_invasive"].astype(str).str.lower() == "yes") | (df_no["covid19_ventilation_non_invasive"].astype(str).str.lower() == "yes")]["id"].astype(int).tolist()

    # df_yes = df_in[df_in["covid19_ventilation"].astype(str).str.lower() == "yes"]
    # failed_ids_yes = df_yes[(df_yes["covid19_ventilation_invasive"].astype(str).str.lower() == "no") & (df_yes["covid19_ventilation_non_invasive"].astype(str).str.lower() == "no")]["id"].astype(int).tolist()

    fail_ids = failed_ids_null + failed_ids_no # + failed_ids_yes

    for fail_id in fail_ids:
        failures[fail_id] = "covid19_ventilation"

    return failures

def repair_covid19_ventilation(df_in):
    used_columns = ["covid19_ventilation", "covid19_admission_hospital", "covid19_ventilation_invasive", "covid19_ventilation_non_invasive"]
    for col in used_columns:
        if col not in df_in.columns:
            print("Unable to clean", col, "as the column is not in the dataframe")
            return df_in

    df_out = df_in.copy()
    df_out["covid19_ventilation_invasive"] = df_out["covid19_ventilation_invasive"].astype(str)
    df_out["covid19_ventilation_non_invasive"] = df_out["covid19_ventilation_non_invasive"].astype(str)

    df_out.loc[(df_out["covid19_ventilation"].isna()) &
               ((df_out["covid19_ventilation_invasive"].astype(str).str.lower() == "yes") | (df_out["covid19_ventilation_invasive"].astype(str).str.lower() == "yes")), "covid19_ventilation"] = "yes"

    df_out.loc[(df_out["covid19_ventilation"].isna()) & (df_out["covid19_admission_hospital"]=="no"), "covid19_ventilation"] = "no"

    return df_out

def repair_covid19_icu(df_in):
    used_columns = ["covid19_icu_stay", "covid19_admission_hospital"]
    for col in used_columns:
        if col not in df_in.columns:
            print("Unable to clean", col, "as the column is not in the dataframe")
            return df_in
    df_out = df_in.copy()
    df_out.loc[(df_out["covid19_icu_stay"].isna()) & (df_out["covid19_admission_hospital"]=="no"), "covid19_icu_stay"] = "no"
    return df_out

def repair_covid19_outcome_death(df_in):
    used_columns = ["covid19_outcome_death", "covid19_outcome_recovered"]
    for col in used_columns:
        if col not in df_in.columns:
            print("Unable to clean", col, "as the column is not in the dataframe")
            return df_in
    df_out = df_in.copy()
    df_out.loc[df_out["covid19_outcome_recovered"]=="yes", "covid19_outcome_death"] = "no"
    return df_out


def repair_sex(df_in):
    df = df_in.copy()
    if "sex" not in df.columns:
        print("Unable to repair sex as the column is not in the dataframe")
        return {}
    df.sex = df.sex.astype(str).astype(str).str.lower()
    return df

def clean_age_years(df_in):
    if "age_years" not in df_in.columns:
        print("Unable to clean age_years as the column is not in the dataframe")
        return {}

    failures = {}

    fail_ids = df_in[(df_in["age_years"]<0) | (df_in["age_years"]>110)]["id"].astype(int).tolist()

    for fail_id in fail_ids:
        failures[fail_id] = "age_years"

    return failures

def clean_pregnancy(df_in):
    if "pregnancy" not in df_in.columns:
        print("Unable to clean pregnancy as the column is not in the dataframe")
        return {}

    failures = {}

    fail_ids = df_in[(df_in["pregnancy"].astype(str).str.lower() == "yes") & (df_in["sex"].astype(str).str.lower() == "male")]["id"].astype(int).tolist()

    for fail_id in fail_ids:
        failures[fail_id] = "pregnancy"

    return failures

def clean_height(df_in):
    if "height" not in df_in.columns:
        print("Unable to clean height as the column is not in the dataframe")
        return {}

    failures = {}

    fail_ids = df_in[(pd.to_numeric(df_in["height"],errors = "coerce")<100) | (pd.to_numeric(df_in["height"],errors = "coerce")>210)]["id"].astype(int).tolist()

    for fail_id in fail_ids:
        failures[fail_id] = "height"

    return failures

def clean_weight(df_in):
    if "weight" not in df_in.columns:
        print("Unable to clean weight as the column is not in the dataframe")
        return {}

    failures = {}

    fail_ids = df_in[(pd.to_numeric(df_in["weight"],errors = "coerce")<30) | (pd.to_numeric(df_in["weight"],errors = "coerce")>300)]["id"].astype(int).tolist()

    for fail_id in fail_ids:
        failures[fail_id] = "weight"

    return failures

def clean_ms_onset_date(df_in):
    used_columns = ["ms_onset_date", "ms_diagnosis_date", "covid19_date_suspected_onset"]
    for col in used_columns:
        if col not in df_in.columns:
            print("Unable to clean", col, "as the column is not in the dataframe")
            return {}

    failures = {}

    fail_ids = df_in[(pd.to_datetime(df_in["ms_onset_date"],errors="coerce")>pd.to_datetime(df_in["ms_diagnosis_date"],errors="coerce")) | (pd.to_datetime(df_in["ms_onset_date"],errors="coerce")>pd.to_datetime(df_in["covid19_date_suspected_onset"],errors="coerce"))]["id"].astype(int).tolist()

    for fail_id in fail_ids:
        failures[fail_id] = "ms_onset_date"
        # print(df_in[df_in["id"] == fail_id][["ms_onset_date", "ms_diagnosis_date", "covid19_date_suspected_onset"]])

    return failures

def clean_edss_date_diagnosis(df_in):
    if "edss_date_diagnosis" not in df_in.columns:
        print("Unable to clean edss_date_diagnosis as the column is not in the dataframe")
        return {}

    failures = {}

    fail_ids = df_in[df_in["edss_date_diagnosis"].apply(lambda d: d.year) < 2019]["id"].astype(int).tolist()

    for fail_id in fail_ids:
        failures[fail_id] = "edss_date_diagnosis"

    return failures

def clean_edss_value(df_in):
    if "edss_value" not in df_in.columns:
        print("Unable to clean edss_value as the column is not in the dataframe")
        return {}

    failures = {}

    fail_ids = df_in[(pd.to_numeric(df_in["edss_value"],errors = "coerce") < 0) | (pd.to_numeric(df_in["edss_value"],errors = "coerce") > 10)]["id"].astype(int).tolist()

    for fail_id in fail_ids:
        failures[fail_id] = "edss_value"

    return failures

def clean_dmt_stop_date(df_in):
    used_columns = ["dmt_stop_date", "current_dmt"]
    for col in used_columns:
        if col not in df_in.columns:
            print("Unable to clean", col, "as the column is not in the dataframe")
            return {}

    failures = {}

    fail_ids = df_in[(df_in["current_dmt"].astype(str).str.lower() == "no") & (pd.to_datetime(df_in["dmt_stop_date"]).apply(lambda d: d.year) < 2019)]["id"].astype(int).tolist()

    for fail_id in fail_ids:
        failures[fail_id] = "dmt_stop_date"

    return failures

def clean_former_smoker(df_in):
    used_columns = ["former_smoker", "current_smoker"]
    for col in used_columns:
        if col not in df_in.columns:
            print("Unable to clean", col, "as the column is not in the dataframe")
            return {}

    failures = {}

    fail_ids = df_in[(df_in["former_smoker"].astype(str).str.lower() == "yes") & (df_in["current_smoker"].astype(str).str.lower() == "yes")]["id"].astype(int).tolist()

    for fail_id in fail_ids:
        failures[fail_id] = ["former_smoker", "current_smoker"]

    return failures

def clean_covid19_outcome_death(df_in):
    used_columns = ["covid19_outcome_death", "covid19_outcome_recovered"]
    for col in used_columns:
        if col not in df_in.columns:
            print("Unable to clean", col, "as the column is not in the dataframe")
            return {}

    failures = {}

    fail_ids = df_in[(df_in["covid19_outcome_death"].astype(str).str.lower() == "yes") & (df_in["covid19_outcome_recovered"].astype(str).str.lower() == "yes")]["id"].astype(int).tolist()

    for fail_id in fail_ids:
        failures[fail_id] = ["covid19_outcome_death", "covid19_outcome_recovered"]

    return failures

def clean_covid19_admission_hospital(df_in):
    used_columns = ["covid19_admission_hospital", "covid19_confirmed_case"]
    for col in used_columns:
        if col not in df_in.columns:
            print("Unable to clean", col, "as the column is not in the dataframe")
            return {}

    failures = {}

    fail_ids = df_in[(df_in["covid19_admission_hospital"].astype(str).str.lower() == "yes") & (df_in["covid19_confirmed_case"].astype(str).str.lower() == "no")]["id"].astype(int).tolist()

    for fail_id in fail_ids:
        failures[fail_id] = "covid19_admission_hospital"

    return failures

def clean_type_dmt(df_in):
    used_columns = ["type_dmt", "type_dmt_other", "current_dmt"]
    for col in used_columns:
        if col not in df_in.columns:
            print("Unable to clean", col, "as the column is not in the dataframe")
            return {}

    failures = {}
    fail_ids = df_in[(df_in["type_dmt"].isna()) & (df_in["type_dmt_other"].isna()) & (df_in["current_dmt"].astype(str).str.lower() == "yes")]["id"].astype(int).tolist()

    for fail_id in fail_ids:
        failures[fail_id] = "type_dmt"

    return failures

def repair_has_comorbidities(df_in):
    if "has_comorbidities" not in df_in.columns:
        print("Unable to repair has_comorbidities as the column is not in the dataframe")
        return df_in

    df_out = df_in.copy()
    comorbidities_columns = [col for col in df_in.columns if col.startswith("com_")]
    df_out[comorbidities_columns] = df_out[comorbidities_columns].astype(str)
    for com_col in comorbidities_columns:
        df_out.loc[df_out[com_col]=="None",com_col] = None
        df_out.loc[df_out[com_col]=="",com_col] = None
        df_out.loc[df_out[com_col]=="nan",com_col] = None

    df_out.loc[df_out["has_comorbidities"].isna() & df_out[comorbidities_columns].apply(lambda c: c.astype(str).str.lower()).apply(lambda r: np.any(r == "yes"),axis=1) == True, "has_comorbidities"] = "yes"


    return df_out

def repair_height_weight(df_in):
    df_out = df_in.copy()
    df_out["height"] = pd.to_numeric(df_in["height"],errors = "coerce")
    df_out["weight"] = pd.to_numeric(df_in["weight"],errors = "coerce")

    return df_out

def clean_height(df_in):
    if "height" not in df_in.columns:
        print("Unable to clean height as the column is not in the dataframe")
        return {}

    failures = {}

    fail_ids = df_in[(pd.to_numeric(df_in["height"],errors = "coerce")<100) | (pd.to_numeric(df_in["height"],errors = "coerce")>210)]["id"].astype(int).tolist()

    for fail_id in fail_ids:
        failures[fail_id] = "height"

    return failures
