import pandas as pd
import numpy as np
from tqdm import tqdm
import matplotlib.pyplot as plt
import pyarrow.parquet as pq
import pyarrow as pa


def allValues(variableStr):

    arrayList = []

    for index, row in tqdm(patients_df.iterrows(), total=nb_patients):
        encounterId = str(row["encounterId"])
        dfPath = dataPath + preProcessedDataPath + encounterId + "/" + variableStr + "_Moy.parquet"
        df = pd.read_parquet(dfPath)
        values = df.to_numpy()[:,0]
        arrayList.append(values)

    tabValues = np.concatenate(arrayList)

    return tabValues

def exportStats(listeVar):

    df_stats = pd.DataFrame()
    
    for variableStr in listeVar:
        
        values = allValues(variableStr)

        mean = np.nanmean(values)
        sd = np.nanstd(values)

        if variableStr.endswith("_I"):
            variableStr = variableStr[:-2]

        stat = pd.DataFrame({variableStr: [mean, sd]})
        
        df_stats[variableStr] = stat.iloc[:, 0]

    df_stats_Path = dataPath + "stats.parquet"
    pq.write_table(pa.Table.from_pandas(df_stats), df_stats_Path)

# dataGroup = "dataECMO"
dataGroup = "dataRea"

if dataGroup == "dataECMO":
    dataPath = "data/"
    patients_df = pd.read_parquet(dataPath + "patients.parquet")
else:
    dataPath = "dataRea/"
    patients_df = pd.read_parquet(dataPath + "patientsRea.parquet")

nb_patients = len(patients_df)

rawDataPath = "rawData/"
preProcessedDataPath = "preProcessedData/"

if dataGroup == "dataRea":
    listeVar = ["HR", "SpO2", "PAD_I", "PAM_I", "PAS_I", "RR", "Temperature", "Diurese", "SpO2_sur_FiO2"]
else:
    listeVar = ["HR", "SpO2", "PAD_I", "PAM_I", "PAS_I", "RR", "Temperature", "Diurese", "SpO2_sur_FiO2", "DebitECMO"]

exportStats(listeVar)