import pandas as pd
import numpy as np
from tqdm import tqdm
import matplotlib.pyplot as plt
import pyarrow.parquet as pq
import pyarrow as pa


def regroupement(dataGroup):

    if dataGroup == "dataECMO":
        dataPath = "dataECMO/"
        listeVar = ["HR", "SpO2", "PAD", "PAM", "PAS", "RR", "Temperature", "Diurese", "SpO2_sur_FiO2", "FiO2", "DebitECMO"]    
    else:
        dataPath = "dataRea/"
        listeVar = ["HR", "SpO2", "PAD", "PAM", "PAS", "RR", "Temperature", "Diurese", "SpO2_sur_FiO2", "FiO2"]

    patients_df = pd.read_parquet(dataPath + "patients.parquet")
    
    preProcessedDataPath = dataPath + "preProcessedData/"
    finalDataPath = dataPath + "finalData/"

    nb_patients = len(patients_df)
    
    df_stats_path = dataPath + "stats.parquet"
    df_stats = pd.read_parquet(df_stats_path)

    for index, row in tqdm(patients_df.iterrows(), total=nb_patients):

        encounterId = str(row["encounterId"])

        df_final = pd.DataFrame()
        
        for variableStr in listeVar:
            
            mean = df_stats[variableStr][0]
            sd = df_stats[variableStr][1]
            mini = df_stats[variableStr][2]
            maxi = df_stats[variableStr][3]

            dfPath = preProcessedDataPath + encounterId + "/" + variableStr + "_Complet" + ".parquet"
            df = pd.read_parquet(dfPath)
            df_final[variableStr] = (df.iloc[:, 0] - mean) / sd
            # df_final[variableStr] = (df.iloc[:, 0] - mini) / (maxi - mini)


        df_final_Path = finalDataPath + encounterId + "/dynamic.parquet"
        pq.write_table(pa.Table.from_pandas(df_final), df_final_Path)


def regroupement_mask(dataGroup):

    if dataGroup == "dataECMO":
        dataPath = "dataECMO/"
        listeVar = ["HR", "SpO2", "PAD", "PAM", "PAS", "RR", "Temperature", "Diurese", "SpO2_sur_FiO2", "FiO2", "DebitECMO"]    
    else:
        dataPath = "dataRea/"
        listeVar = ["HR", "SpO2", "PAD", "PAM", "PAS", "RR", "Temperature", "Diurese", "SpO2_sur_FiO2", "FiO2"]
    
    patients_df = pd.read_parquet(dataPath + "patients.parquet")

    preProcessedDataPath = dataPath + "preProcessedData/"
    finalDataPath = dataPath + "finalData/"

    nb_patients = len(patients_df)

    for index, row in tqdm(patients_df.iterrows(), total=nb_patients):

        encounterId = str(row["encounterId"])

        df_final = pd.DataFrame()
        
        for variableStr in listeVar:

            dfPath = preProcessedDataPath + encounterId + "/" + variableStr + "_Mask" + ".parquet"
            df = pd.read_parquet(dfPath)
            df_final[variableStr] = df

        df_final_Path = finalDataPath + encounterId + "/mask.parquet"
        pq.write_table(pa.Table.from_pandas(df_final), df_final_Path)


def regroupement_static(dataGroup):
    
    if dataGroup == "dataECMO":
        dataPath = "dataECMO/"
    else:
        dataPath = "dataRea/"
        
    patients_df = pd.read_parquet(dataPath + "patients.parquet")
        
    preProcessedDataPath = dataPath + "preProcessedData/"
    finalDataPath = dataPath + "finalData/"

    nb_patients = len(patients_df)

    for index, row in tqdm(patients_df.iterrows(), total=nb_patients):

        encounterId = str(row["encounterId"])

        df_final_static = pd.DataFrame()

        age = int(patients_df.loc[patients_df["encounterId"]==int(encounterId),"patientAge"].to_numpy()[0])
        df_final_static['age'] = [(age - 18) / (93 - 18)]

        sexe = str(patients_df.loc[patients_df["encounterId"]==int(encounterId),"patientGender"].to_numpy()[0])
        if sexe == "Masculin":
            df_final_static['sexe'] = [-1]
        else:
            df_final_static['sexe'] = [1]

        mean_IMC = 27.204   #Stats sur Réa Rangueil
        sd_IMC = 6.135
        min_IMC = 10.84
        max_IMC = 65.53
        dfPath = preProcessedDataPath + encounterId + "/IMC" + ".parquet"
        df = pd.read_parquet(dfPath)
        df_final_static['IMC'] = (df.iloc[:, 0] - mean_IMC) / sd_IMC
        # df_final_static['IMC'] = (df.iloc[:, 0] - min_IMC) / (max_IMC - min_IMC)


        df_final_Path = finalDataPath + encounterId + "/static.parquet"
        pq.write_table(pa.Table.from_pandas(df_final_static), df_final_Path)


# dataGroup = "dataECMO"
# dataGroup = "dataRea"

# regroupement(dataGroup)
# regroupement_mask(dataGroup)
# regroupement_static(dataGroup)