import pandas as pd
import numpy as np
from tqdm import tqdm
import matplotlib.pyplot as plt
import pyarrow.parquet as pq
import pyarrow as pa


# concatène toutes les valeurs d'une variable
def allValues(variableStr, dataPath, preProcessedDataPath, patients_df):

    nb_patients = len(patients_df)
    
    arrayList = []

    for index, row in tqdm(patients_df.iterrows(), total=nb_patients):
        encounterId = str(row["encounterId"])
        dfPath = dataPath + preProcessedDataPath + encounterId + "/" + variableStr + "_Moy.parquet"
        df = pd.read_parquet(dfPath)
        values = df.to_numpy()[:,0]
        arrayList.append(values)

    tabValues = np.concatenate(arrayList)

    return tabValues


# création d'un fichier avec la moyenne, l'écart-type, le minimum, le maximum des variables d'un groupe de patient
def exportStats(dataGroup):

    if dataGroup == "dataECMO":
        dataPath = "dataECMO/"
    else:
        dataPath = "dataRea/"
    
    patients_df = pd.read_parquet(dataPath + "patients.parquet")

    nb_patients = len(patients_df)

    rawDataPath = "rawData/"
    preProcessedDataPath = "preProcessedData/"

    if dataGroup == "dataRea":
        listeVar = ["HR", "SpO2", "PAD_I", "PAM_I", "PAS_I", "RR", "Temperature", "Diurese", "SpO2_sur_FiO2", "FiO2"]
    else:
        listeVar = ["HR", "SpO2", "PAD_I", "PAM_I", "PAS_I", "RR", "Temperature", "Diurese", "SpO2_sur_FiO2", "FiO2", "DebitECMO"]
    
    df_stats = pd.DataFrame()
    
    for variableStr in listeVar:
        
        values = allValues(variableStr, dataPath, preProcessedDataPath, patients_df)

        mean = np.nanmean(values)
        sd = np.nanstd(values)
        mini = np.nanmin(values)
        maxi = np.nanmax(values)

        if variableStr.endswith("_I"):
            variableStr = variableStr[:-2]

        stat = pd.DataFrame({variableStr: [mean, sd, mini, maxi]})
        
        df_stats[variableStr] = stat.iloc[:, 0]

    df_stats_Path = dataPath + "stats.parquet"
    pq.write_table(pa.Table.from_pandas(df_stats), df_stats_Path)


# dataGroup = "dataECMO"
# dataGroup = "dataRea"

# exportStats(dataGroup)