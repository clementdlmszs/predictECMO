import pandas as pd
import numpy as np
from tqdm import tqdm
import matplotlib.pyplot as plt
import datetime

dataPath = "data/"
preProcessedDataPath = "preProcessedData/"

patients_df = pd.read_parquet(dataPath + "patients.parquet")

nb_patients = len(patients_df)


def percentageMissingValuesPerPatient():

    variablesStr = ["HR", "RR", "PAD_I", "PAS_I", "PAM_I", "SpO2", "Temperature", "Diurese"]
    
    listPercentageMissingValues = []
    listPatients = []

    for _, row in tqdm(patients_df.iterrows(), total=nb_patients):

        encounterId = str(row["encounterId"])
        
        numTotal = 0
        numMissingValues = 0
        
        for variableStr in variablesStr:
            dfPath = preProcessedDataPath + encounterId + "/" + variableStr + "_Moy.parquet"
            df = pd.read_parquet(dfPath)

            numTotal += df.iloc[:,0].size
            numMissingValues += df.iloc[:,0].isna().sum()
        
        listPercentageMissingValues.append(numMissingValues/numTotal*100)
        listPatients.append(encounterId)

        newdf = pd.DataFrame({'patients': listPatients, 'pourcentageValeursManquantes': listPercentageMissingValues})

        newDfPath = "dataAnalysis/statsValeursManquantesParPatientSansPA_NI.csv"
        newdf.to_csv(newDfPath, index=False)


def countPatientsHighMissingValues(treshold):
    path = "dataAnalysis/statsValeursManquantesParPatientSansPA_NI.csv"
    data = np.genfromtxt(path, delimiter=',', skip_header=1)
    return np.sum(data[:,1] > treshold)


# percentageMissingValuesPerPatient()
count = countPatientsHighMissingValues(35)
print(count)