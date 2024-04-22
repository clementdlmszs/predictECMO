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

        dfPath = preProcessedDataPath + encounterId + "/Weight.parquet"
        dfPath2 = preProcessedDataPath + encounterId + "/Weight2.parquet"
        
        df = pd.read_parquet(dfPath)
        df2 = pd.read_parquet(dfPath2)
        
        numeric_values = pd.to_numeric(df.iloc[:, 0], errors='coerce').dropna()
        numeric_values2 = pd.to_numeric(df2.iloc[:, 0], errors='coerce').dropna()

        if df.size == 0 and df2.size == 0:
            meanWeight = 80
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


def gestionTaille():
     for index, row in tqdm(patients_df.iterrows(), total=nb_patients):

        encounterId = str(row["encounterId"])

        dfPath = preProcessedDataPath + encounterId + "/Height.parquet"

        df = pd.read_parquet(dfPath)
        
        numeric_values = pd.to_numeric(df.iloc[:, 0], errors='coerce').dropna()

        if df.size == 0:
            meanHeight = 175
        else:
            meanHeight = np.mean(numeric_values)
        
        newdf = pd.DataFrame([meanHeight])

        newDfPath = preProcessedDataPath + encounterId + "/Height_Moy.parquet"
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

        newDfPath = preProcessedDataPath + encounterId + "/Diurese_Moy.parquet"
        pq.write_table(pa.Table.from_pandas(newdf), newDfPath)



def moyenne_sur_x_minutes(variableStr, frequenceAcquisition, columnValuesStr):
    
    for index, row in tqdm(patients_df.iterrows(), total=nb_patients):

        encounterId = str(row["encounterId"])

        dfPath = preProcessedDataPath + encounterId + "/" + variableStr + ".parquet"
        
        df = pd.read_parquet(dfPath)

        sizeDf = df[columnValuesStr].size
        new_index = range(sizeDf)
        df.index = new_index

        liste_valeurs = []
        
        # Test si le df est vide
        if sizeDf > 0:
            lastTime = int(df['temps'].iloc[-1] // frequenceAcquisition)
        else:
            lastTime = 0

        nbValeurs = df.index.max()+1

        # On calcule la valeur moyenne de la variable d'intérêt sur un intervalle de temps donné en paramètre
        index_current_time = 0
        for i in range(lastTime):
            valeur_moy = 0
            current_time = df['temps'][index_current_time]
            compteur = 0
            while (current_time < (i+1)*frequenceAcquisition) and (index_current_time < nbValeurs):
                valeur_moy += df[columnValuesStr][index_current_time] 

                index_current_time += 1
                current_time = df['temps'][index_current_time]
                compteur += 1
            
            if compteur > 0:
                liste_valeurs.append(valeur_moy/compteur)
            else:
                liste_valeurs.append(np.nan)

        
        liste_temps = list(range(0,lastTime*frequenceAcquisition,frequenceAcquisition))

        newdf = pd.DataFrame({columnValuesStr: liste_valeurs, 'temps': liste_temps})

        newDfPath = preProcessedDataPath + encounterId + "/" + variableStr + "_Moy.parquet"
        pq.write_table(pa.Table.from_pandas(newdf), newDfPath)

gestionPoids()
gestionTaille()
gestionDiurese(6)
moyenne_sur_x_minutes("HR", 60, "HR")
moyenne_sur_x_minutes("SpO2", 60, "SpO2")
moyenne_sur_x_minutes("PAD_I", 60, "pad_i")
moyenne_sur_x_minutes("PAM_I", 60, "pam_i")
moyenne_sur_x_minutes("PAS_I", 60, "pas_i")
moyenne_sur_x_minutes("RR", 60, "RR")
moyenne_sur_x_minutes("Temperature", 60, "temperature")
moyenne_sur_x_minutes("DebitECMO", 60, "debit")
moyenne_sur_x_minutes("PAD_NI", 60, 'pad_ni')
moyenne_sur_x_minutes("PAM_NI", 60, 'pam_ni')
moyenne_sur_x_minutes("PAS_NI", 60, 'pas_ni')
