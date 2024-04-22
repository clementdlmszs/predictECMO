import pandas as pd
import numpy as np
from tqdm import tqdm
import matplotlib.pyplot as plt
import pyarrow.parquet as pq
import pyarrow as pa


dataPath = "data/"
preProcessedDataPath = "preProcessedData/"
patients_df = pd.read_parquet(dataPath + "patients.parquet")

nb_patients = len(patients_df)


def gestionDonneesAberrantes(variableStr, minTheorique, maxTheorique, columnStr):
    
    for index, row in tqdm(patients_df.iterrows(), total=nb_patients):

        encounterId = str(row["encounterId"])

        dfPath = dataPath + encounterId + "/" + variableStr + ".parquet"
        df = pd.read_parquet(dfPath)
        
        df_filtered_nan = df[~pd.isna(df[columnStr])]

        df_filtered = df_filtered_nan[(df_filtered_nan[columnStr] >= minTheorique) & (df_filtered_nan[columnStr] <= maxTheorique)]

        newDfPath = preProcessedDataPath + encounterId + "/" + variableStr + ".parquet"
        pq.write_table(pa.Table.from_pandas(df_filtered), newDfPath)


# gestionDonneesAberrantes("HR", 20, 200, 'HR')
# gestionDonneesAberrantes("Death", 0, 1, 'Death')
# gestionDonneesAberrantes("SpO2", 50, 100, 'SpO2')
# gestionDonneesAberrantes("PAD_I", 1, 200, 'pad_i')
# gestionDonneesAberrantes("PAM_I", 10, 200, 'pam_i')
# gestionDonneesAberrantes("PAS_I", 1, 200, 'pas_i')
# gestionDonneesAberrantes("PAD_NI", 1, 200, 'pad_ni')
# gestionDonneesAberrantes("PAM_NI", 10, 200, 'pam_ni')
# gestionDonneesAberrantes("PAS_NI", 1, 200, 'pas_ni')
# gestionDonneesAberrantes("RR", 5, 60, 'RR')
# gestionDonneesAberrantes("Temperature", 28, 42, 'temperature')
# gestionDonneesAberrantes("DebitECMO", 0, 10, 'debit')
# gestionDonneesAberrantes("Diurese",-1,10,'diurese_heure')
# gestionDonneesAberrantes("Weight",30, 300, 'weight')
# gestionDonneesAberrantes("Weight2",30, 300, 'weight')
# gestionDonneesAberrantes("Height", 100, 230, 'height')