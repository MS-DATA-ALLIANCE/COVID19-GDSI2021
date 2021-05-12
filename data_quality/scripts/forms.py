from qmenta.core.platform import Auth, post, parse_response
from getpass import getpass
import pandas as pd
import numpy as np
import datetime
import utils
import utils_cleaning
from utils import enhance_forms_data



if __name__ =="__main__":
    fail_value = None
    project_id_forms = 3150

    # base url to connect to the central platform
    base_url = "https://platform.qmenta.com"
    # PUT YOUR USERNAME (EMAIL) HERE
    username = "clement.gautrais@kuleuven.be"
    # you will be asked for your password here
    password = getpass()

    # creation of authentication object
    auth_obj = Auth.login(username, password, base_url)

    # method to fetch the subjects data
    def get_subjects_data(project_id):
        r = post(auth_obj, "/patient_manager/get_patient_list",
                {"_pid":project_id},
                timeout=600.0)

        data_trans = [{
                "id": record["_id"],
                "secret_name":record["patient_secret_name"],
                **{
                    k[3:]:record[k]
                    for k in record
                    if k[:3] == "md_"
                }
        } for record in parse_response(r)]

        for r in data_trans:
            for k in r:
                if isinstance(r[k], dict):
                    try:
                        r[k] = datetime.datetime.fromtimestamp(r[k]["$date"]/1000.0)
                    except:
                        r[k] = np.nan

        return data_trans

    data_reg = get_subjects_data(project_id_forms)
    df = pd.DataFrame(data_reg)

    clean_df = utils_cleaning.clean_data(df, auth_obj, project_id_forms, fail_value)
    df_e = enhance_forms_data(clean_df)
