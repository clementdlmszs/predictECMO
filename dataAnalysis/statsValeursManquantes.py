import pandas as pd
import numpy as np
from tqdm import tqdm
import matplotlib.pyplot as plt
import datetime

dataPath = "data/"
preProcessedDataPath = "preProcessedData/"

patients_df = pd.read_parquet(dataPath + "patients.parquet")

nb_patients = len(patients_df)


def percentageMissingValues(variableStr):

    numTotal = 0
    numMissingValues = 0

    for _, row in patients_df.iterrows():
        
        encounterId = str(row["encounterId"])
        dfPath = preProcessedDataPath + encounterId + "/" + variableStr + ".parquet"
        df = pd.read_parquet(dfPath)

        numTotal += df.iloc[:,0].size
        numMissingValues += df.iloc[:,0].isna().sum()

    return numMissingValues/numTotal*100

    
missingValues_HR = percentageMissingValues("HR_Moy")
print("Percentage of missing values for HR: " + str(missingValues_HR))

missingValues_RR = percentageMissingValues("RR_Moy")
print("Percentage of missing values for RR: " + str(missingValues_RR))

missingValues_PAD_I = percentageMissingValues("PAD_I_Moy")
print("Percentage of missing values for PAD_I: " + str(missingValues_PAD_I))

missingValues_PAS_I = percentageMissingValues("PAS_I_Moy")
print("Percentage of missing values for PAS_I: " + str(missingValues_PAS_I))

missingValues_PAM_I = percentageMissingValues("PAM_I_Moy")
print("Percentage of missing values for PAM_I: " + str(missingValues_PAM_I))

missingValues_PAD_NI = percentageMissingValues("PAD_NI_Moy")
print("Percentage of missing values for PAD_NI: " + str(missingValues_PAD_NI))

missingValues_PAS_NI = percentageMissingValues("PAS_NI_Moy")
print("Percentage of missing values for PAS_NI: " + str(missingValues_PAS_NI))

missingValues_PAM_NI = percentageMissingValues("PAM_NI_Moy")
print("Percentage of missing values for PAM_NI: " + str(missingValues_PAM_NI))

missingValues_SpO2 = percentageMissingValues("SpO2_Moy")
print("Percentage of missing values for SpO2: " + str(missingValues_SpO2))

missingValues_Temperature = percentageMissingValues("Temperature_Moy")
print("Percentage of missing values for Temperature: " + str(missingValues_Temperature))

missingValues_DebitECMO = percentageMissingValues("DebitECMO_Moy")
print("Percentage of missing values for DebitECMO: " + str(missingValues_DebitECMO))

missingValues_Diurese = percentageMissingValues("Diurese_Moy")
print("Percentage of missing values for Diuresis: " + str(missingValues_Diurese))