import pandas as pd
import numpy as np
from tqdm import tqdm
import matplotlib.pyplot as plt
import pyarrow.parquet as pq
import pyarrow as pa


def delai_deces(dataGroup):

    if dataGroup == "dataECMO":
        dataPath = "data/"
        patients_df = pd.read_parquet(dataPath + "patients.parquet")
        deces_df = pd.read_csv(dataPath + "patientsECMO_IPP_deces.csv")
    else:
        dataPath = "dataRea/"
        patients_df = pd.read_parquet(dataPath + "patientsRea.parquet")
        deces_df = pd.read_csv(dataPath + "patientsIPP_ReaRangueil_deces.csv")

    nb_patients = len(patients_df)

    encounterIds = []
    delais_installation = []
    delais_withdrawal = []

    for index, row in tqdm(patients_df.iterrows(), total=nb_patients):
        
        encounterId = str(row["encounterId"])
        patientLifetimeNumber = str(row["patientLifetimeNumber"])

        is_deceased = str(deces_df.loc[deces_df["patientLifetimeNumber"]==int(patientLifetimeNumber),"code_deces"].to_numpy()[0])

        if is_deceased:
            installation_date = pd.Timestamp(row["installation_date"])
            withdrawal_date = pd.Timestamp(row["withdrawal_date"])

            date_deces = pd.Timestamp(deces_df.loc[deces_df["patientLifetimeNumber"]==int(patientLifetimeNumber),"date_deces"].to_numpy()[0])

            delai_installation = (date_deces - installation_date).total_seconds() / (60 * 60 * 24)
            delai_withdrawal = (date_deces - withdrawal_date).total_seconds() / (60 * 60 * 24)

            delais_installation.append(delai_installation)
            delais_withdrawal.append(delai_withdrawal)
        else:
            delais_installation.append(np.nan)
            delais_withdrawal.append(np.nan)
        
        encounterIds.append(encounterId)
    
    df_delais = pd.DataFrame({'encounterId': encounterIds, 'delai_installation_deces': delais_installation, 'delai_sortie_deces': delais_withdrawal})

    df_delais_Path = dataPath + "delais_deces.csv"
    df_delais.to_csv(df_delais_Path, index=False)



dataGroup = "dataECMO"
# dataGroup = "dataRea"

delai_deces(dataGroup)