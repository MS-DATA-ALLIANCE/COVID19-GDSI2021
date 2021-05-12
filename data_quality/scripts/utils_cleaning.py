import sys
import numpy as np
import pandas as pd

sys.path.insert(1, '../../')

from data_quality.scripts import cleaning_functions

def set_qa_status(auth_obj, project_id, patient_id, status, comments):
    from qmenta.core.platform import Auth, post, parse_response
    r = post(auth_obj, "/projectset_manager/set_qa_status",
             {"_pid": project_id,
              "item_ids": patient_id,
              "status": status,
              "comments": comments,
              "entity": "patients"},
             timeout=600.0)
    return parse_response(r)["success"] == 1

def update_all_failures(failures, all_failures):
    for fail_id, variables in failures.items():
        if fail_id not in all_failures:
            all_failures[fail_id] = []
        if isinstance(variables, list):
            all_failures[fail_id].extend(variables)
        else:
            all_failures[fail_id].append(variables)

def send_all_qa_status(all_failures, auth_obj, all_ids, project_id):
    for patient_id in all_ids:
        if patient_id in all_failures:
            answer = set_qa_status(auth_obj, project_id, patient_id, "fail", ",".join(all_failures[patient_id]))
        else:
            answer = set_qa_status(auth_obj, project_id, patient_id, "pass", "")
        if not answer:
            print(f"Failed to set qa status fail for patient_id={patient_id}")

def flag_failures_in_df(df_in, all_failures, fail_value=None):
    df = df_in.copy()
    for patient_id, failures in all_failures.items():
        df.loc[df["id"] == patient_id, failures] = fail_value

    return df

def clean_data(df_in, auth_obj, project_id, fail_value=None, send_qa_staus=False):
    df = df_in.copy()

    df.replace(regex=r'^\s*$', value=np.nan, inplace=True) # Replaces empty strings by np.nan
    df = df.where(pd.notnull(df), None) # replaces null values with None

    all_failures = {}
    update_all_failures(cleaning_functions.clean_covid19_date_reporting(df), all_failures)
    update_all_failures(cleaning_functions.clean_year_reporting(df), all_failures)
    update_all_failures(cleaning_functions.clean_covid19_has_symptoms(df), all_failures)
    update_all_failures(cleaning_functions.clean_covid19_self_isolation_date(df), all_failures)
    # update_all_failures(cleaning_functions.clean_covid19_self_isolation_duration(df), all_failures)
    update_all_failures(cleaning_functions.clean_covid19_date_lab_test(df), all_failures)
    update_all_failures(cleaning_functions.clean_covid19_date_suspected_onset(df), all_failures)
    update_all_failures(cleaning_functions.clean_covid19_admission_hospital_release(df), all_failures)
    update_all_failures(cleaning_functions.check_all_dates(df), all_failures)
    update_all_failures(cleaning_functions.clean_covid19_icu_stay(df), all_failures)
    update_all_failures(cleaning_functions.clean_covid19_ventilation(df), all_failures)
    update_all_failures(cleaning_functions.clean_age_years(df), all_failures)
    update_all_failures(cleaning_functions.clean_pregnancy(df), all_failures)
    update_all_failures(cleaning_functions.clean_height(df), all_failures)
    update_all_failures(cleaning_functions.clean_weight(df), all_failures)
    update_all_failures(cleaning_functions.clean_ms_onset_date(df), all_failures)
    # update_all_failures(cleaning_functions.clean_edss_date_diagnosis(df), all_failures)
    update_all_failures(cleaning_functions.clean_edss_value(df), all_failures)
    # update_all_failures(cleaning_functions.clean_dmt_stop_date(df), all_failures)
    update_all_failures(cleaning_functions.clean_former_smoker(df), all_failures)
    update_all_failures(cleaning_functions.clean_covid19_outcome_death(df), all_failures)
    update_all_failures(cleaning_functions.clean_type_dmt(df), all_failures)
    # update_all_failures(cleaning_functions.clean_covid19_admission_hospital(df), all_failures)

    if send_qa_staus:
        send_all_qa_status(all_failures, auth_obj, df["id"].astype(int).tolist(), project_id)



    df_flagged = flag_failures_in_df(df_in, all_failures, fail_value)

    return repair_data(df_flagged)

def repair_data(df_in):
    df_res = cleaning_functions.repair_covid19_has_symptoms(df_in)
    df_res = cleaning_functions.repair_covid19_ventilation(df_res)
    df_res = cleaning_functions.repair_has_comorbidities(df_res)
    df_res = cleaning_functions.repair_sex(df_res)
    df_res = cleaning_functions.repair_type_dmt_other(df_res)
    df_res = cleaning_functions.repair_covid19_icu(df_res)
    df_res = cleaning_functions.repair_covid19_outcome_death(df_res)

    # The astype(str) converts np.nan to 'nan', so we revert this (it is a known pandas bug)
    df_res = df_res.replace('nan', np.nan)
    return df_res
