import pandas as pd
import numpy as np
from tqdm import tqdm
import matplotlib.pyplot as plt
import pyarrow.parquet as pq
import pyarrow as pa
from datetime import datetime

# dataPath = "data/"
# preProcessedDataPath = "data/preProcessedData/"
# patients_df = pd.read_parquet(dataPath + "patients.parquet")

dataPath = "dataRea/"
preProcessedDataPath = "dataRea/preProcessedData/"
patients_df = pd.read_parquet(dataPath + "patientsRea.parquet")

nb_patients = len(patients_df)

def gestionValeursManquantes(variableStr, columnValuesStr, defaultValue):

    for index, row in tqdm(patients_df.iterrows(), total=nb_patients):

            encounterId = str(row["encounterId"])

            dfPath = preProcessedDataPath + encounterId + "/" + variableStr + "_Moy" + ".parquet"
            
            df = pd.read_parquet(dfPath)

            sizeDf = df[columnValuesStr].size

            # La taille des listes est fixée au nombre d'heures total du séjour (+4) 
            withdrawal_date = pd.Timestamp(row["withdrawal_date"])
            installation_date = pd.Timestamp(row["installation_date"])
            total_time_hour = (withdrawal_date - installation_date).total_seconds() / 3600 + 4

            liste_valeurs = []
            liste_mask = [] # permet de savoir si une valeur est manquante à l'origine
            
            if df[columnValuesStr].isnull().all() or sizeDf == 0:
                liste_valeurs = [defaultValue] * int(total_time_hour)
                liste_mask = [0] * int(total_time_hour)
                sizeDf = 0
            
            if sizeDf > 0:
                for i in range(int(total_time_hour)):

                    if i >= sizeDf: # plus de valeurs existantes à partir de là
                        val_i = np.nan
                    else:
                        val_i = df[columnValuesStr][i]

                    if np.isnan(val_i):
                        
                        liste_mask.append(0) # On crée une valeur qui n'existe pas
                        
                        j = 1
                        found = False
                        
                        # On associe la valeur la plus proche
                        # En cas d'equidistance la moyenne entre les 2 valeurs les plus proches
                        while not(found):
                            if i-j < 0:
                                while i+j < sizeDf:
                                    val_i_plus_j = df[columnValuesStr][i+j]
                                    if not(np.isnan(val_i_plus_j)):
                                        new_val = val_i_plus_j
                                        found = True
                                        break
                                    j += 1
                            elif i+j >= sizeDf:
                                while i-j >= sizeDf:
                                    j += 1
                                while i-j >= 0:
                                    val_i_moins_j = df[columnValuesStr][i-j]
                                    if not(np.isnan(val_i_moins_j)):
                                        new_val = val_i_moins_j
                                        found = True
                                        break
                                    j += 1
                            else:
                                val_i_moins_j = df[columnValuesStr][i-j]
                                val_i_plus_j = df[columnValuesStr][i+j]
                                if not(np.isnan(val_i_moins_j)):
                                    if not(np.isnan(val_i_plus_j)):
                                        new_val = (val_i_moins_j + val_i_plus_j) * 0.5
                                    else:
                                        new_val = val_i_moins_j
                                    found = True
                                elif not(np.isnan(val_i_plus_j)):
                                    new_val = val_i_plus_j
                                    found = True
                            j += 1
                        
                        liste_valeurs.append(new_val)

                    else:
                        liste_mask.append(1) # La valeur existe
                        liste_valeurs.append(val_i)
            

            newdf = pd.DataFrame({columnValuesStr: liste_valeurs})
            newdfPath = preProcessedDataPath + encounterId + "/" + variableStr + "_Complet.parquet"
            pq.write_table(pa.Table.from_pandas(newdf), newdfPath)

            newdfMask = pd.DataFrame({'mask': liste_mask})
            newdfMaskPath = preProcessedDataPath + encounterId + "/" + variableStr + "_Mask.parquet"
            pq.write_table(pa.Table.from_pandas(newdfMask), newdfMaskPath)



# Fonction à part pour les pressions artérielles
# On associe d'abord la PAI si elle existe, sinon la PANI si elle existe, sinon la PAI la plus proche si elle existe, sinon la PANI la plus proche
def gestionValeursManquantesPA(PA_Str, columnValuesStr, defaultValue):

    PA_I_Str = PA_Str + "_I"
    PA_NI_str = PA_Str + "_NI"

    for index, row in tqdm(patients_df.iterrows(), total=nb_patients):
            
            columnValuesStr_PAI = columnValuesStr + "_i"
            columnValuesStr_PANI = columnValuesStr + "_ni"

            encounterId = str(row["encounterId"])

            dfPath = preProcessedDataPath + encounterId + "/" + PA_I_Str + "_Moy" + ".parquet"
            dfPath2 = preProcessedDataPath + encounterId + "/" + PA_NI_str + "_Moy" + ".parquet"

            df = pd.read_parquet(dfPath)
            df2 = pd.read_parquet(dfPath2)

            sizeDf = df[columnValuesStr_PAI].size
            sizeDf2 = df2[columnValuesStr_PANI].size

            withdrawal_date = pd.Timestamp(row["withdrawal_date"])
            installation_date = pd.Timestamp(row["installation_date"])
            total_time_hour = (withdrawal_date - installation_date).total_seconds() / 3600 + 4

            liste_valeurs = []
            liste_mask = [] # permet de savoir si une valeur est manquante à l'origine

            if df[columnValuesStr_PAI].isnull().all() or sizeDf == 0:
                if df2[columnValuesStr_PANI].isnull().all() or sizeDf2 == 0:
                    liste_valeurs = [defaultValue] * int(total_time_hour)
                    liste_mask = [0] * int(total_time_hour)
                    sizeDf = 0
                else:
                    df = df2
                    columnValuesStr_PAI = columnValuesStr_PANI
                    sizeDf = sizeDf2

            if sizeDf > 0:
                for i in range(int(total_time_hour)):

                    if i >= sizeDf:
                        val_i = np.nan
                    else:
                        val_i = df[columnValuesStr_PAI][i]

                    if np.isnan(val_i):

                        if (i < sizeDf2) and (not(np.isnan(df2[columnValuesStr_PANI][i]))):
                            new_val = df2[columnValuesStr_PANI][i]
                            liste_valeurs.append(new_val)
                        
                        else:
                            
                            liste_mask.append(0)

                            j = 1
                            found = False
                            
                            while not(found):
                                if i-j < 0:
                                    while i+j < sizeDf:
                                        val_i_plus_j = df[columnValuesStr_PAI][i+j]
                                        if not(np.isnan(val_i_plus_j)):
                                            new_val = val_i_plus_j
                                            found = True
                                            break
                                        j += 1
                                elif i+j >= sizeDf:
                                    while i-j >= sizeDf:
                                        j += 1
                                    while i-j >= 0:
                                        val_i_moins_j = df[columnValuesStr_PAI][i-j]
                                        if not(np.isnan(val_i_moins_j)):
                                            new_val = val_i_moins_j
                                            found = True
                                            break
                                        j += 1
                                else:
                                    val_i_moins_j = df[columnValuesStr_PAI][i-j]
                                    val_i_plus_j = df[columnValuesStr_PAI][i+j]
                                    if not(np.isnan(val_i_moins_j)):
                                        if not(np.isnan(val_i_plus_j)):
                                            new_val = (val_i_moins_j + val_i_plus_j) * 0.5
                                        else:
                                            new_val = val_i_moins_j
                                        found = True
                                    elif not(np.isnan(val_i_plus_j)):
                                        new_val = val_i_plus_j
                                        found = True
                                j += 1
                            
                            liste_valeurs.append(new_val)

                    else:
                        liste_mask.append(1)
                        liste_valeurs.append(val_i)
            

            newdf = pd.DataFrame({columnValuesStr: liste_valeurs})
            newDfPath = preProcessedDataPath + encounterId + "/" + columnValuesStr.upper() + "_Complet.parquet"
            pq.write_table(pa.Table.from_pandas(newdf), newDfPath)

            newdfMask = pd.DataFrame({'mask': liste_mask})
            newdfMaskPath = preProcessedDataPath + encounterId + "/" + columnValuesStr.upper() + "_Mask.parquet"
            pq.write_table(pa.Table.from_pandas(newdfMask), newdfMaskPath)


# gestionValeursManquantes("HR", "HR", 85)
# gestionValeursManquantes("SpO2", "SpO2", 96)
# gestionValeursManquantesPA("PAD", "pad", 60)
# gestionValeursManquantesPA("PAM", "pam", 80)
# gestionValeursManquantesPA("PAS", "pas", 125)
# gestionValeursManquantes("RR", "RR", 22)
# gestionValeursManquantes("Temperature", "temperature", 37)
# gestionValeursManquantes("DebitECMO", "debit", 3)
# gestionValeursManquantes("Diurese", "diurese", 0.0015)
# gestionValeursManquantes("SpO2_sur_FiO2", 'SpO2_sur_FiO2', 2.5)


# gestionValeursManquantes("FiO2", "FiO2", 40) # Facultatif