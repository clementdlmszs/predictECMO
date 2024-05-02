import pandas as pd
import numpy as np
from tqdm import tqdm
import matplotlib.pyplot as plt
import pyarrow.parquet as pq
import pyarrow as pa


dataGroup = "dataECMO"
# dataGroup = "dataRea"

if dataGroup == "dataECMO":
    dataPath = "data/"
    patients_df = pd.read_parquet(dataPath + "patients.parquet")
else:
    dataPath = "dataRea/"
    patients_df = pd.read_parquet(dataPath + "patientsRea.parquet")

preProcessedDataPath = dataPath + "preProcessedData/"
finalDataPath = dataPath + "finalData/"

nb_patients = len(patients_df)


def regroupement(listeVar):
    for index, row in tqdm(patients_df.iterrows(), total=nb_patients):

        encounterId = str(row["encounterId"])

        df_final = pd.DataFrame()
        
        for variableStr in listeVar:
            
            dfPath = preProcessedDataPath + encounterId + "/" + variableStr + "_Complet" + ".parquet"
            df = pd.read_parquet(dfPath)
            df_final[variableStr] = df.iloc[:, 0]

        df_final_Path = finalDataPath + encounterId + "/" + encounterId + ".parquet"
        pq.write_table(pa.Table.from_pandas(df_final), df_final_Path)


if dataGroup == "dataRea":
    listeVar = ["HR", "SpO2", "PAD", "PAM", "PAS", "RR", "Temperature", "Diurese", "SpO2_sur_FiO2"]
else:
    listeVar = ["HR", "SpO2", "PAD", "PAM", "PAS", "RR", "Temperature", "Diurese", "SpO2_sur_FiO2", "DebitECMO"]


regroupement(listeVar)