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

def gestionValeursManquantes(variableStr, columnValuesStr):

    for index, row in tqdm(patients_df.iterrows(), total=nb_patients):

            encounterId = str(row["encounterId"])

            dfPath = preProcessedDataPath + encounterId + "/" + variableStr + ".parquet"
            
            df = pd.read_parquet(dfPath)

            sizeDf = df[columnValuesStr].size
            
            liste_valeurs = []

            for i in range(sizeDf):
                val_i = df[columnValuesStr][i]

                if val_i is None:
                    j = 1
                    found = False
                    while not(found):
                        if i-j < 0:
                            while i+j < sizeDf:
                                val_i_plus_j = df[columnValuesStr][i+j]
                                if not(val_i_plus_j is None):
                                    new_val = val_i_plus_j
                                    found = True
                                    break
                                j += 1
                        elif i+j > sizeDf:
                            while i-j >= 0:
                                val_i_moins_j = df[columnValuesStr][i-j]
                                if not(val_i_moins_j is None):
                                    new_val = val_i_moins_j
                                    found = True
                                    break
                                j += 1
                        else:
                            val_i_moins_j = df[columnValuesStr][i-j]
                            val_i_plus_j = df[columnValuesStr][i+j]
                            if not(val_i_moins_j is None):
                                if not(val_i_plus_j is None):
                                    new_val = (val_i_moins_j + val_i_plus_j) * 0.5
                                else:
                                    new_val = val_i_moins_j
                                found = True
                            elif not(val_i_plus_j is None):
                                new_val = val_i_plus_j
                                found = True
                        j += 1
                    
                    liste_valeurs.append(new_val)

                else:

                    liste_valeurs.append(val_i)
            

            newdf = pd.DataFrame({columnValuesStr: liste_valeurs})

            newDfPath = preProcessedDataPath + encounterId + "/" + variableStr + "_Complet.parquet"
            pq.write_table(pa.Table.from_pandas(newdf), newDfPath)

gestionValeursManquantes("HR_Moy", "HR")