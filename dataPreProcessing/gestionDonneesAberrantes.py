import pandas as pd
import numpy as np
from tqdm import tqdm
import matplotlib.pyplot as plt
import pyarrow.parquet as pq
import pyarrow as pa


dataPath = "dataRea/"
patients_df = pd.read_parquet(dataPath + "patientsRea.parquet")

nb_patients = len(patients_df)


def gestionDonneesAberrantes(variableStr, minTheorique, maxTheorique, columnStr):
    
    for index, row in tqdm(patients_df.iterrows(), total=nb_patients):

        encounterId = str(row["encounterId"])

        dfPath = dataPath + "rawData/" + encounterId + "/" + variableStr + ".parquet"
        df = pd.read_parquet(dfPath)
        
        df_filtered_nan = df[~pd.isna(df[columnStr])]

        df_filtered = df_filtered_nan[(df_filtered_nan[columnStr] >= minTheorique) & (df_filtered_nan[columnStr] <= maxTheorique)]

        newDfPath = dataPath + "preProcessedData/" + encounterId + "/" + variableStr + ".parquet"
        pq.write_table(pa.Table.from_pandas(df_filtered), newDfPath)


# gestionDonneesAberrantes("HR", 20, 200, 'HR')
# gestionDonneesAberrantes("SpO2", 50, 100, 'SpO2')
# gestionDonneesAberrantes("PAD_I", 20, 130, 'pad_i')
# gestionDonneesAberrantes("PAM_I", 30, 200, 'pam_i')
# gestionDonneesAberrantes("PAS_I", 40, 230, 'pas_i')
# gestionDonneesAberrantes("PAD_NI", 20, 130, 'pad_ni')
# gestionDonneesAberrantes("PAM_NI", 30, 200, 'pam_ni')
# gestionDonneesAberrantes("PAS_NI", 40, 230, 'pas_ni')
# gestionDonneesAberrantes("RR", 5, 50, 'RR')
# gestionDonneesAberrantes("Temperature", 32, 41, 'temperature')
# gestionDonneesAberrantes("DebitECMO", 0, 8, 'debit')
# gestionDonneesAberrantes("Diurese", 0, 2,'diurese_heure')
# gestionDonneesAberrantes("Weight", 30, 300, 'weight')
# gestionDonneesAberrantes("Weight2", 30, 300, 'weight')
# gestionDonneesAberrantes("Height", 120, 230, 'height')
gestionDonneesAberrantes("FiO2", 20, 100, 'FiO2')