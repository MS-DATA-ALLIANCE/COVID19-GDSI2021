import pandas as pd
import datetime

def oui_non_to_yes_no(df_in, variable_french, variable_created):
    df_out = df_in.copy()
    df_out[variable_created] = ""

    if variable_french not in df_in.columns:
        print(f"Unable to create {variable_created}, {variable_french} is not in the dataframe")
        return df_out

    variable_yes_indices = df_out[variable_french].str.lower() == "oui"
    df_out.loc[variable_yes_indices, variable_created] = "yes"

    variable_no_indices = df_out[variable_french].str.lower() == "non"
    df_out.loc[variable_no_indices, variable_created] = "no"

    return df_out

def map_date(df_in, variable_french, variable_created):
    df_out = df_in.copy()
    df_out[variable_created] = ""

    if variable_french not in df_in.columns:
        print(f"Unable to create {variable_created}, {variable_french} is not in the dataframe")
        return df_out


    df_out.loc[:, variable_created] = pd.to_datetime(df_out[variable_french].astype(str).apply(lambda s: s.replace("DM", "01")), errors="coerce")

    return df_out

def create_covid19_sympt_fever(df_in):
    return oui_non_to_yes_no(df_in, "Fièvre >38°c", "covid19_sympt_fever")

def create_covid19_sympt_dry_cough(df_in):
    return oui_non_to_yes_no(df_in, "Toux", "covid19_sympt_dry_cough")

def create_covid19_sympt_fatigue(df_in):
    return oui_non_to_yes_no(df_in, "Asthénie", "covid19_sympt_fatigue")

def create_covid19_sympt_shortness_breath(df_in):
    return oui_non_to_yes_no(df_in, "Gêne respiratoire", "covid19_sympt_shortness_breath")

def create_covid19_sympt_loss_smell_taste(df_in):
    return oui_non_to_yes_no(df_in, "Anosmie/Agueusie", "covid19_sympt_loss_smell_taste")

def create_covid19_confirmed_case(df_in):
    df_out = df_in.copy()
    df_out["covid19_confirmed_case"] = ""

    if "PCR COVID" not in df_in.columns:
        print("Unable to create covid19_confirmed_case, PCR COVID is not in the dataframe")
        return df_out

    variable_yes_indices = df_out["PCR COVID"].str.lower() == "positif"
    df_out.loc[variable_yes_indices, "covid19_confirmed_case"] = "yes"

    variable_no_indices = (df_out["PCR COVID"].str.lower() == "negatif") | (df_out["PCR COVID"].str.lower() == "non fait") | (df_out["PCR COVID"].str.lower() == "négatif")
    df_out.loc[variable_no_indices, "covid19_confirmed_case"] = "no"

    return df_out

def create_covid19_admission_hospital(df_in):
    df_out = df_in.copy()
    df_out["covid19_admission_hospital"] = ""

    variable_french = "Cotation de l’état clinique (à l’aide de l’échelle ordinale ci-contre) : Pour les patients SEP de cette étude, nous vous proposons de coter seulement le Nadir de l’évolution clinique."

    if variable_french not in df_in.columns:
        print(f"Unable to create covid19_admission_hospital, {variable_french} is not in the dataframe")
        return df_out

    df_out[variable_french] = df_out[variable_french].astype(str)

    variable_no_indices = (df_out[variable_french] == "Non hospitalisé, aucune limitation des activités") | (df_out[variable_french] == "Non hospitalisé, limitation des activités")
    df_out.loc[variable_no_indices, "covid19_admission_hospital"] = "no"

    variable_yes_indices = (df_out[variable_french] == "Hospitalisé, ne nécessitant pas d'oxygène supplémentaire") | (df_out[variable_french] == "Hospitalisé, nécessitant de l'oxygène supplémentaire") | (df_out[variable_french] == "Hospitalisé, sur une ventilation non invasive ou des dispositifs d'oxygène à haut débit") | (df_out[variable_french] == "Hospitalisé, sous ventilation mécanique invasive ou oxygénation par membrane extra-corporelle")
    df_out.loc[variable_yes_indices, "covid19_admission_hospital"] = "yes"

    return df_out

def create_covid19_ventilation(df_in):
    df_out = df_in.copy()
    df_out["covid19_ventilation"] = ""

    variable_french = "Cotation de l’état clinique (à l’aide de l’échelle ordinale ci-contre) : Pour les patients SEP de cette étude, nous vous proposons de coter seulement le Nadir de l’évolution clinique."

    if variable_french not in df_in.columns:
        print(f"Unable to create covid19_ventilation, {variable_french} is not in the dataframe")
        return df_out

    df_out[variable_french] = df_out[variable_french].astype(str)

    variable_no_indices = df_out[variable_french] == "Hospitalisé, ne nécessitant pas d'oxygène supplémentaire"
    df_out.loc[variable_no_indices, "covid19_ventilation"] = "no"

    variable_yes_indices = (df_out[variable_french] == "Hospitalisé, nécessitant de l'oxygène supplémentaire") | (df_out[variable_french] == "Hospitalisé, sur une ventilation non invasive ou des dispositifs d'oxygène à haut débit") | (df_out[variable_french] == "Hospitalisé, sous ventilation mécanique invasive ou oxygénation par membrane extra-corporelle")
    df_out.loc[variable_yes_indices, "covid19_ventilation"] = "yes"

    return df_out

def create_covid19_ventilation_non_invasive(df_in):
    df_out = df_in.copy()
    df_out["covid19_ventilation_non_invasive"] = ""

    variable_french = "Cotation de l’état clinique (à l’aide de l’échelle ordinale ci-contre) : Pour les patients SEP de cette étude, nous vous proposons de coter seulement le Nadir de l’évolution clinique."

    if variable_french not in df_in.columns:
        print(f"Unable to create covid19_ventilation_non_invasive, {variable_french} is not in the dataframe")
        return df_out

    df_out[variable_french] = df_out[variable_french].astype(str)

    variable_yes_indices = (df_out[variable_french] == "Hospitalisé, sur une ventilation non invasive ou des dispositifs d'oxygène à haut débit") | (df_out[variable_french] == "Hospitalisé, nécessitant de l'oxygène supplémentaire")
    df_out.loc[variable_yes_indices, "covid19_ventilation_non_invasive"] = "yes"

    variable_no_indices = (df_out[variable_french] == "Hospitalisé, sous ventilation mécanique invasive ou oxygénation par membrane extra-corporelle")
    df_out.loc[variable_no_indices, "covid19_ventilation_non_invasive"] = "no"

    return df_out

def create_covid19_ventilation_invasive(df_in):
    df_out = df_in.copy()
    df_out["covid19_ventilation_invasive"] = ""

    variable_french = "Cotation de l’état clinique (à l’aide de l’échelle ordinale ci-contre) : Pour les patients SEP de cette étude, nous vous proposons de coter seulement le Nadir de l’évolution clinique."

    if variable_french not in df_in.columns:
        print(f"Unable to create covid19_ventilation_invasive, {variable_french} is not in the dataframe")
        return df_out

    df_out[variable_french] = df_out[variable_french].astype(str)

    variable_no_indices = (df_out[variable_french] == "Hospitalisé, sur une ventilation non invasive ou des dispositifs d'oxygène à haut débit") | (df_out[variable_french] == "Hospitalisé, nécessitant de l'oxygène supplémentaire")
    df_out.loc[variable_no_indices, "covid19_ventilation_invasive"] = "no"

    variable_yes_indices = df_out[variable_french] == "Hospitalisé, sous ventilation mécanique invasive ou oxygénation par membrane extra-corporelle"
    df_out.loc[variable_yes_indices, "covid19_ventilation_invasive"] = "yes"

    return df_out

def create_covid19_ecmo(df_in):
    df_out = df_in.copy()
    df_out["covid19_ecmo"] = ""

    variable_french = "Cotation de l’état clinique (à l’aide de l’échelle ordinale ci-contre) : Pour les patients SEP de cette étude, nous vous proposons de coter seulement le Nadir de l’évolution clinique."

    if variable_french not in df_in.columns:
        print(f"Unable to create covid19_ecmo, {variable_french} is not in the dataframe")
        return df_out

    df_out[variable_french] = df_out[variable_french].astype(str)

    variable_no_indices = (df_out[variable_french] == "Hospitalisé, sur une ventilation non invasive ou des dispositifs d'oxygène à haut débit") | (df_out[variable_french] == "Hospitalisé, nécessitant de l'oxygène supplémentaire")
    df_out.loc[variable_no_indices, "covid19_ecmo"] = "no"

    variable_yes_indices = df_out[variable_french] == "Hospitalisé, sous ventilation mécanique invasive ou oxygénation par membrane extra-corporelle"
    df_out.loc[variable_yes_indices, "covid19_ecmo"] = "yes"

    return df_out

def create_covid19_outcome_death(df_in):
    df_out = df_in.copy()
    df_out["covid19_outcome_death"] = ""

    variable_french = "Cotation de l’état clinique (à l’aide de l’échelle ordinale ci-contre) : Pour les patients SEP de cette étude, nous vous proposons de coter seulement le Nadir de l’évolution clinique."

    if variable_french not in df_in.columns:
        print(f"Unable to create covid19_outcome_death, {variable_french} is not in the dataframe")
        return df_out

    df_out[variable_french] = df_out[variable_french].astype(str)

    variable_yes_indices = df_out[variable_french] == "Mort"
    df_out.loc[variable_yes_indices, "covid19_outcome_death"] = "yes"

    return df_out

def create_age_years(df_in):
    df_out = df_in.copy()
    df_out["age_years"] = ""

    variable_french = "Date de naissance (MM/AAAA)"

    if variable_french not in df_in.columns:
        print(f"Unable to create age_years, {variable_french} is not in the dataframe")
        return df_out

    current_year = datetime.datetime.now().year

    df_years = pd.to_datetime(df_out[variable_french], errors="coerce")
    df_out.loc[:, "age_years"] = df_years.apply(lambda d: current_year-d.year)

    return df_out

def create_sex(df_in):
    df_out = df_in.copy()
    df_out["sex"] = ""

    variable_french = "Sexe"

    if variable_french not in df_in.columns:
        print(f"Unable to create sex, {variable_french} is not in the dataframe")
        return df_out


    df_out.loc[:, "sex"] = df_out[variable_french].map({"Masculin": "male", "Féminin": "female"})

    return df_out

def create_ms_type(df_in):
    df_out = df_in.copy()
    df_out["ms_type"] = ""

    variable_french = "Forme de SEP"

    if variable_french not in df_in.columns:
        print(f"Unable to create ms_type, {variable_french} is not in the dataframe")
        return df_out


    df_out.loc[:, "ms_type"] = df_out[variable_french].map({"RIS": "", "CIS": "CIS", "SEP RR (non CIS)": "RRMS", "SEP PP":"PPMS", "SEP SP":"SPMS"})

    return df_out

def create_ms_onset_date(df_in):
    df_out = df_in.copy()
    df_out["ms_onset_date"] = ""

    variable_french = "Date début de SEP"

    if variable_french not in df_in.columns:
        print(f"Unable to create ms_onset_date, {variable_french} is not in the dataframe")
        return df_out


    df_out.loc[:, "ms_onset_date"] = pd.to_datetime(df_out[variable_french].astype(str).apply(lambda s: s.replace("DM", "01")), errors="coerce")

    return df_out

def create_edss_value(df_in):
    df_out = df_in.copy()
    df_out["edss_value"] = ""

    variable_french = "EDSS"

    if variable_french not in df_in.columns:
        print(f"Unable to create edss_value, {variable_french} is not in the dataframe")
        return df_out

    df_out.loc[:, "edss_value"] = pd.to_numeric(df_out[variable_french], errors="coerce", downcast="float")

    return df_out

def create_has_comorbidities(df_in):
    df_out = df_in.copy()
    variable_created = "has_comorbidities"
    df_out[variable_created] = ""

    variables_french = ["Comorbidités (choice=Cardiaque)",
                        "Comorbidités (choice=Pulmonaire)",
                        "Comorbidités (choice=Diabète)",
                        "Comorbidités (choice=Tabagisme en cours)",
                        "Comorbidités (choice=Obésité (BMI>30))",
                        "Comorbidités (choice=Autre)",
                        "Comorbidités (choice=Aucune)"]

    for variable_french in variables_french:
        if variable_french not in df_in.columns:
            print(f"Unable to create {variable_created}, {variable_french} is not in the dataframe")
            return df_out

    variable_yes_indices = df_out[variables_french[0]].str.lower() == "checked"
    for variable_french in variables_french[1:-1]:
        variable_yes_indices |= df_out[variable_french].str.lower() == "checked"
    df_out.loc[variable_yes_indices, variable_created] = "yes"

    variable_no_indices = df_out[variables_french[-1]].str.lower() == "checked"
    df_out.loc[variable_no_indices, variable_created] = "no"

    return df_out

def create_com_cardiovascular_disease(df_in):
    df_out = df_in.copy()
    variable_created = "com_cardiovascular_disease"
    df_out[variable_created] = ""

    variables_french = [
        "Comorbidités (choice=Cardiaque)",
        "Comorbidité cardiaque (choice=Cardiopathie Ischémique)",
        "Comorbidité cardiaque (choice=Autre)"
    ]

    for variable_french in variables_french:
        if variable_french not in df_in.columns:
            print(f"Unable to create {variable_created}, {variable_french} is not in the dataframe")
            return df_out

    variable_yes_indices = df_out[variables_french[0]].str.lower() == "checked"
    # variable_yes_indices &= df_out["Comorbidité cardiaque (choice=HTA)"].str.lower() == "checked"
    for variable_french in variables_french[1:]:
        variable_yes_indices |= df_out[variable_french].str.lower() == "checked"
    df_out.loc[variable_yes_indices, variable_created] = "yes"

    variable_no_indices = df_out[variables_french[0]].str.lower() == "unchecked"
    for variable_french in variables_french[1:]:
        variable_no_indices &= df_out[variable_french].str.lower() == "unchecked"
    df_out.loc[variable_no_indices, variable_created] = "no"

    return df_out

def create_com_hypertension(df_in):
    df_out = df_in.copy()
    variable_created = "com_hypertension"
    df_out[variable_created] = ""

    variable_french = "Comorbidité cardiaque (choice=HTA)"

    if variable_french not in df_in.columns:
        print(f"Unable to create {variable_created}, {variable_french} is not in the dataframe")
        return df_out

    variable_yes_indices = df_out[variable_french].str.lower() == "checked"
    df_out.loc[variable_yes_indices, variable_created] = "yes"

    variable_no_indices = df_out[variable_french].str.lower() == "unchecked"
    df_out.loc[variable_no_indices, variable_created] = "no"

    return df_out

def create_com_diabetes(df_in):
    df_out = df_in.copy()
    variable_created = "com_diabetes"
    df_out[variable_created] = ""

    variable_french = "Comorbidités (choice=Diabète)"

    if variable_french not in df_in.columns:
        print(f"Unable to create {variable_created}, {variable_french} is not in the dataframe")
        return df_out

    variable_yes_indices = df_out[variable_french].str.lower() == "checked"
    df_out.loc[variable_yes_indices, variable_created] = "yes"

    variable_no_indices = df_out[variable_french].str.lower() == "unchecked"
    df_out.loc[variable_no_indices, variable_created] = "no"

    return df_out

def create_com_lung_disease(df_in):
    df_out = df_in.copy()
    variable_created = "com_lung_disease"
    df_out[variable_created] = ""

    variables_french = [
        "Comorbidités (choice=Pulmonaire)",
        "Comorbidité Pulmonaire (choice=BPCO)",
        "Comorbidité Pulmonaire (choice=Asthme)",
        "Comorbidité Pulmonaire (choice=Autre)"
    ]

    for variable_french in variables_french:
        if variable_french not in df_in.columns:
            print(f"Unable to create {variable_created}, {variable_french} is not in the dataframe")
            return df_out

    variable_yes_indices = df_out[variables_french[0]].str.lower() == "checked"
    for variable_french in variables_french[1:]:
        variable_yes_indices |= df_out[variable_french].str.lower() == "checked"
    df_out.loc[variable_yes_indices, variable_created] = "yes"

    variable_no_indices = df_out[variables_french[0]].str.lower() == "unchecked"
    for variable_french in variables_french[1:]:
        variable_no_indices &= df_out[variable_french].str.lower() == "unchecked"
    df_out.loc[variable_no_indices, variable_created] = "no"

    return df_out

def create_com_other(df_in):
    df_out = df_in.copy()
    variable_created = "com_other"
    df_out[variable_created] = ""

    variables_french = [
        "Comorbidités (choice=Autre)",
        "Comorbidités (choice=Obésité (BMI>30))",
        "Comorbidités (choice=Tabagisme en cours)"
    ]

    for variable_french in variables_french:
        if variable_french not in df_in.columns:
            print(f"Unable to create {variable_created}, {variable_french} is not in the dataframe")
            return df_out

    variable_obesity_indices = df_out["Comorbidités (choice=Obésité (BMI>30))"].str.lower() == "checked"
    df_out.loc[variable_obesity_indices, variable_created] = "Obesity"

    variable_smoker_indices = df_out["Comorbidités (choice=Tabagisme en cours)"].str.lower() == "checked"
    variable_smoker_indices &= ~variable_obesity_indices
    df_out.loc[variable_smoker_indices, variable_created] = "Smoker"

    variable_yes_indices = df_out["Comorbidités (choice=Autre)"].str.lower() == "checked"
    variable_yes_indices &= ~variable_obesity_indices
    variable_yes_indices &= ~variable_smoker_indices
    df_out.loc[variable_yes_indices, variable_created] = df_out["Si autre, préciser"].loc[variable_yes_indices]

    return df_out

def create_current_smoker(df_in):
    df_out = df_in.copy()
    variable_created = "current_smoker"
    df_out[variable_created] = ""

    variable_french = "Comorbidités (choice=Tabagisme en cours)"

    if variable_french not in df_in.columns:
        print(f"Unable to create {variable_created}, {variable_french} is not in the dataframe")
        return df_out

    variable_yes_indices = df_out[variable_french].str.lower() == "checked"
    df_out.loc[variable_yes_indices, variable_created] = "yes"

    variable_no_indices = df_out[variable_french].str.lower() == "unchecked"
    df_out.loc[variable_no_indices, variable_created] = "no"

    return df_out

def create_current_dmt(df_in):
    df_out = df_in.copy()
    df_out["current_dmt"] = ""

    variable_french = "Traitement actuel SEP"

    if variable_french not in df_in.columns:
        print(f"Unable to create current_dmt, {variable_french} is not in the dataframe")
        return df_out

    no_treatment_indices = df_out[variable_french] == "Aucun Traitement"
    yes_treatment_indices = ~no_treatment_indices & (df_out[variable_french] != "") & (~(df_out[variable_french].isna()))

    df_out.loc[no_treatment_indices, "current_dmt"] = "no"
    df_out.loc[yes_treatment_indices, "current_dmt"] = "yes"

    return df_out

def create_type_dmt(df_in):
    df_out = df_in.copy()
    df_out["type_dmt"] = ""

    variable_french = "Traitement actuel SEP"

    if variable_french not in df_in.columns:
        print(f"Unable to create type_dmt, {variable_french} is not in the dataframe")
        return df_out

    treatment_mapping = {
        "Interferon beta": "interferons",
        "Glatiramere": "glatiramer",
        "Teriflunomide": "teriflunomide",
        "Dimethylfumarate": "dimethyl_fumarate",
        "Fingolimod": "fingolimod",
        "Natalizumab": "natalizumab",
        "Ocrelizumab": "ocrelizumab",
        "Rituximab": "rituximab",
        "Siponimod": "siponimod",
        "Cladribine": "cladribine",
        "Alemtuzumab": "alemtuzumab"
    }

    df_out.loc[:, "type_dmt"] = df_out[variable_french].map(treatment_mapping)

    return df_out

def create_dmt_glucocorticoid(df_in):
    return oui_non_to_yes_no(df_in, "Traitement par corticoïdes dans les 30j qui précèdent l'infection", "dmt_glucocorticoid")

def create_covid19_date_suspected_onset(df_in):
    return map_date(df_in, "Date de début des symptômes", "covid19_date_suspected_onset")

def create_dmt_start_date(df_in):
    return map_date(df_in, "Date de début du traitement actuel", "dmt_start_date")

def create_dmt_end_date(df_in):
    return map_date(df_in, "Si traitement par cure, date de la dernière cure", "dmt_end_date")

def create_dmt_glucocorticoid_start_date(df_in):
    return map_date(df_in, "Date de début", "dmt_glucocorticoid_start_date")

def create_dmt_glucocorticoid_stop_date(df_in):
    return map_date(df_in, "Date de fin", "dmt_glucocorticoid_stop_date")

def create_type_dmt_other(df_in):
    df_out = df_in.copy()
    df_out["type_dmt_other"] = ""

    variable_french = "Si traitement SEP autre, préciser :"

    if variable_french not in df_in.columns:
        print(f"Unable to create type_dmt_other, {variable_french} is not in the dataframe")
        return df_out

    df_out.loc[:, "type_dmt_other"] = df_out[variable_french]

    return df_out

def create_covid19_country(df_in):
    df_out = df_in.copy()
    df_out["covid19_country"] = "France"

    return df_out

def create_dmt_stop_date(df_in):
    df_out = df_in.copy()
    df_out["dmt_stop_date"] = ""

    variables_french = ["Modification du traitement de fond de la SEP/NMO", "Si modification, ", "Date d'arrêt ou diminution"]
    for variable_french in variables_french:
        if variable_french not in df_in.columns:
            print(f"Unable to create dmt_stop_date, {variable_french} is not in the dataframe")
            return df_out

    indices_stop = (df_out["Modification du traitement de fond de la SEP/NMO"].str.lower() == "oui") & (df_out["Si modification, "].astype(str) == "Arrêt")
    df_out.loc[indices_stop, "dmt_stop_date"] = pd.to_datetime(df_out["Date d'arrêt ou diminution"].astype(str).apply(lambda s: s.replace("DM", "01")), errors="coerce")

    return df_out

def create_id_column(df_in):
    df_out = df_in.copy()
    df_out["id"] = range(df_in.shape[0])
    return df_out

def filter_out_nmo_patients(df_in):
    df_out = df_in.copy()
    variable_french = "Diagnostic de maladie inflammatoire du SNC"

    if variable_french not in df_in.columns:
        print(f"Unable to remove nmo patients, {variable_french} is not in the dataframe")
        return df_out
    return df_out.loc[df_out[variable_french]=="SEP", :]

def transform_french_df(df):
    transformed_columns = ["covid19_sympt_fever", "covid19_sympt_dry_cough", "covid19_sympt_fatigue",
                           "covid19_sympt_shortness_breath", "covid19_sympt_loss_smell_taste", "covid19_confirmed_case",
                           "covid19_admission_hospital", "covid19_ventilation", "covid19_outcome_death", "age_years",
                           "sex", "ms_type", "ms_onset_date", "edss_value", "has_comorbidities",
                           "com_cardiovascular_disease", "com_hypertension", "com_diabetes", "com_lung_disease",
                           "com_other", "current_dmt", "type_dmt", "dmt_glucocorticoid", "covid19_date_suspected_onset",
                           "covid19_ventilation_non_invasive", "covid19_ventilation_invasive", "covid19_ecmo",
                           "dmt_start_date", "dmt_end_date", "dmt_glucocorticoid_start_date", "dmt_glucocorticoid_stop_date",
                           "current_smoker", "type_dmt_other", "covid19_country", "dmt_stop_date", "id"]

    empty_columns = ["covid19_sympt_sore_throat", "covid19_sympt_pneumonia",
                     "covid19_sympt_pain", "covid19_sympt_nasal_congestion", "covid19_sympt_chills"]
    yes_columns = ["covid19_suspected_case"]
    new_df = df.copy()

    for col in empty_columns:
        new_df[col] = ""
    for col in yes_columns:
        new_df[col] = "yes"

    new_df = create_id_column(new_df)
    new_df = filter_out_nmo_patients(new_df)
    new_df = create_covid19_sympt_fever(new_df)
    new_df = create_covid19_sympt_dry_cough(new_df)
    new_df = create_covid19_sympt_fatigue(new_df)
    new_df = create_covid19_sympt_shortness_breath(new_df)
    new_df = create_covid19_sympt_loss_smell_taste(new_df)
    new_df = create_covid19_confirmed_case(new_df)
    new_df = create_covid19_admission_hospital(new_df)
    new_df = create_covid19_ventilation(new_df)
    new_df = create_covid19_outcome_death(new_df)
    new_df = create_age_years(new_df)
    new_df = create_sex(new_df)
    new_df = create_ms_type(new_df)
    new_df = create_ms_onset_date(new_df)
    new_df = create_edss_value(new_df)
    new_df = create_has_comorbidities(new_df)
    new_df = create_com_cardiovascular_disease(new_df)
    new_df = create_com_hypertension(new_df)
    new_df = create_com_diabetes(new_df)
    new_df = create_com_lung_disease(new_df)
    new_df = create_com_other(new_df)
    new_df = create_current_dmt(new_df)
    new_df = create_type_dmt(new_df)
    new_df = create_dmt_glucocorticoid(new_df)
    new_df = create_covid19_date_suspected_onset(new_df)
    new_df = create_covid19_ventilation_non_invasive(new_df)
    new_df = create_covid19_ventilation_invasive(new_df)
    new_df = create_covid19_ecmo(new_df)
    new_df = create_dmt_start_date(new_df)
    new_df = create_dmt_stop_date(new_df)
    new_df = create_dmt_end_date(new_df)
    new_df = create_dmt_glucocorticoid_start_date(new_df)
    new_df = create_dmt_glucocorticoid_stop_date(new_df)
    new_df = create_current_smoker(new_df)
    new_df = create_type_dmt_other(new_df)
    new_df = create_covid19_country(new_df)

    return new_df[transformed_columns + empty_columns + yes_columns]

if __name__ == "__main__":
    french_df = pd.read_csv("data/CoviSEP_DATA_LABELS_msif.csv")

    transformed_df = transform_french_df(french_df)
    transformed_df.to_csv("data/transformed_df.csv", index=False)