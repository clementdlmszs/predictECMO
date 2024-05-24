import pandas as pd
import numpy as np
from tqdm import tqdm
import matplotlib.pyplot as plt
import pyarrow.parquet as pq
import pyarrow as pa


def gestionDonneesAberrantes(dataGroup, variableStr, minTheorique, maxTheorique, columnStr):
    
    if dataGroup == "dataECMO":
        dataPath = "data/"
    else:
        dataPath = "dataRea/"
        
    patients_df = pd.read_parquet(dataPath + "patients.parquet")

    nb_patients = len(patients_df)

    for index, row in tqdm(patients_df.iterrows(), total=nb_patients):

        encounterId = str(row["encounterId"])

        dfPath = dataPath + "rawData/" + encounterId + "/" + variableStr + ".parquet"
        df = pd.read_parquet(dfPath)
        
        df_filtered_nan = df[~pd.isna(df[columnStr])]

        df_filtered = df_filtered_nan[(df_filtered_nan[columnStr] >= minTheorique) & (df_filtered_nan[columnStr] <= maxTheorique)]

        newDfPath = dataPath + "preProcessedData/" + encounterId + "/" + variableStr + ".parquet"
        pq.write_table(pa.Table.from_pandas(df_filtered), newDfPath)


# dataGoup = "dataECMO"
dataGroup = "dataRea"

# gestionDonneesAberrantes(dataGroup, "HR", 20, 200, 'HR')
# gestionDonneesAberrantes(dataGroup, "SpO2", 50, 100, 'SpO2')
# gestionDonneesAberrantes(dataGroup, "PAD_I", 20, 130, 'pad_i')
# gestionDonneesAberrantes(dataGroup, "PAD_NI", 20, 130, 'pad_ni')
# gestionDonneesAberrantes(dataGroup, "PAM_I", 30, 200, 'pam_i')
# gestionDonneesAberrantes(dataGroup, "PAM_NI", 30, 200, 'pam_ni')
# gestionDonneesAberrantes(dataGroup, "PAS_I", 40, 230, 'pas_i')
# gestionDonneesAberrantes(dataGroup, "PAS_NI", 40, 230, 'pas_ni')
# gestionDonneesAberrantes(dataGroup, "RR", 5, 50, 'RR')
# gestionDonneesAberrantes(dataGroup, "Temperature", 32, 41, 'temperature')
# gestionDonneesAberrantes(dataGroup, "DebitECMO", 0, 8, 'debit')
# gestionDonneesAberrantes(dataGroup, "Diurese", 0, 2,'diurese_heure')
# gestionDonneesAberrantes(dataGroup, "Weight", 30, 300, 'weight')
# gestionDonneesAberrantes(dataGroup, "Weight2", 30, 300, 'weight')
# gestionDonneesAberrantes(dataGroup, "Height", 120, 230, 'height')
# gestionDonneesAberrantes(dataGroup, "FiO2", 20, 100, 'FiO2')