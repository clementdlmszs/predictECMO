import pandas as pd
import numpy as np
from tqdm import tqdm
import matplotlib.pyplot as plt
import pyarrow.parquet as pq
import pyarrow as pa
from datetime import datetime

dataPath = "data/"
preProcessedDataPath = "preProcessedData/"
patients_df = pd.read_parquet(dataPath + "patients.parquet")

nb_patients = len(patients_df)

def gestionValeursManquantes(variableStr, columnValuesStr):

    for index, row in tqdm(patients_df.iterrows(), total=nb_patients):

            encounterId = str(row["encounterId"])

            dfPath = preProcessedDataPath + encounterId + "/" + variableStr + "_Moy" + ".parquet"
            
            df = pd.read_parquet(dfPath)

            sizeDf = df[columnValuesStr].size

            withdrawal_date = pd.Timestamp(row["withdrawal_date"])
            installation_date = pd.Timestamp(row["installation_date"])
            total_time_hour = (withdrawal_date - installation_date).total_seconds() / 3600 + 4

            liste_valeurs = []

            if sizeDf > 0:
                for i in range(int(total_time_hour)):

                    if i >= sizeDf:
                        val_i = np.nan
                    else:
                        val_i = df[columnValuesStr][i]

                    if np.isnan(val_i):
                        j = 1
                        found = False
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

                        liste_valeurs.append(val_i)
            

            newdf = pd.DataFrame({columnValuesStr: liste_valeurs})

            newDfPath = preProcessedDataPath + encounterId + "/" + variableStr + "_Complet.parquet"
            pq.write_table(pa.Table.from_pandas(newdf), newDfPath)

# gestionValeursManquantes("HR", "HR")
gestionValeursManquantes("SpO2", "SpO2")
# gestionValeursManquantes("PAD_I", "pad_i")
# gestionValeursManquantes("PAM_I", "pam_i")
# gestionValeursManquantes("PAS_I", "pas_i")
# gestionValeursManquantes("RR", "RR")
# gestionValeursManquantes("Temperature", "temperature")
# gestionValeursManquantes("DebitECMO", "debit")
# gestionValeursManquantes("Diurese", "diurese")
