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


def gestionPoids():
    
    for index, row in tqdm(patients_df.iterrows(), total=nb_patients):

        encounterId = str(row["encounterId"])

        dfPath = dataPath + encounterId + "/Weight.parquet"
        dfPath2 = dataPath + encounterId + "/Weight2.parquet"
        
        df = pd.read_parquet(dfPath)
        df2 = pd.read_parquet(dfPath2)
        
        numeric_values = pd.to_numeric(df.iloc[:, 0], errors='coerce').dropna()
        numeric_values2 = pd.to_numeric(df2.iloc[:, 0], errors='coerce').dropna()

        if df.size == 0 and df2.size == 0:
            meanWeight = 70
        elif df.size == 0:
            meanWeight = np.mean(numeric_values2)
        elif df2.size == 0:
            meanWeight = np.mean(numeric_values)
        else:
            arrayValues = np.concatenate((np.array(numeric_values), np.array(numeric_values2)))
            meanWeight = np.mean(arrayValues)

        newdf = pd.DataFrame([meanWeight])

        newDfPath = preProcessedDataPath + encounterId + "/Weight3.parquet"
        pq.write_table(pa.Table.from_pandas(newdf), newDfPath)


def gestionDiurese(h_for_avg):
    
    for index, row in tqdm(patients_df.iterrows(), total=nb_patients):

        encounterId = str(row["encounterId"])

        dfPath = preProcessedDataPath + encounterId + "/Diurese.parquet"
        
        df = pd.read_parquet(dfPath)
        
        weight = pd.read_parquet(preProcessedDataPath+encounterId+"/Weight3.parquet").to_numpy()[0]

        liste_diurese_moy = []
        liste_temps = []
        
        for i in range(h_for_avg,df['temps'].size):
            temps_i = df['temps'].iloc[i]
            valide = True
            diurese_moy = 0
            for j in range(h_for_avg):
                if df['temps'].iloc[i-j] == (temps_i - 60*j):
                    diurese_moy += df['diurese_heure'].iloc[i-j]
                else:
                    valide = False
                    break
            diurese_moy = diurese_moy/h_for_avg/weight
            if valide:
                liste_diurese_moy.append(diurese_moy)
                liste_temps.append(temps_i)

        newdf = pd.DataFrame({'diurese': liste_diurese_moy, 'temps': liste_temps})

        newDfPath = preProcessedDataPath + encounterId + "/DiureseMoy.parquet"
        pq.write_table(pa.Table.from_pandas(newdf), newDfPath)


def gestionHR():
    
    for index, row in tqdm(patients_df.iterrows(), total=nb_patients):

        encounterId = str(row["encounterId"])

        dfPath = preProcessedDataPath + encounterId + "/HR.parquet"
        
        df = pd.read_parquet(dfPath)
        
        hr = pd.read_parquet(dataPath+encounterId+"/HR.parquet").to_numpy()[0]

        liste_hr = []
        liste_temps = []
        
        lastTime = int(df['temps'].iloc[-1] // 60)
        time = df['temps'][0]

        for i in range(lastTime):
            hr_moy = 0
            j = i
            time_j = df['temps'][j]
            compteur = 0
            while time_j < (i+1)*60:
                hr_moy += df['HR']
                compteur += 1
            
            if compteur > 0:
                liste_hr.append(hr_moy)
            else:
                liste_hr.append(np.nan)

        newdf = pd.DataFrame({'diurese': liste_diurese_moy, 'temps': liste_temps})

        newDfPath = preProcessedDataPath + encounterId + "/DiureseMoy.parquet"
        pq.write_table(pa.Table.from_pandas(newdf), newDfPath)


# gestionPoids()
# gestionDiurese(6)