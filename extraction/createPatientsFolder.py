import pandas as pd
import os

dataPath = "dataRea/"
filePatients = "patientsRea.parquet"

patients_df = pd.read_parquet(dataPath + filePatients)

# Création d'un dossier pour chaque encounterId différent
for encounterId in patients_df["encounterId"]:

    folderPath = dataPath + "rawData/" + str(encounterId)

    if not os.path.exists(folderPath):
        os.makedirs(folderPath)


for encounterId in patients_df["encounterId"]:

    folderPath = dataPath + "preProcessedData/" + str(encounterId)

    if not os.path.exists(folderPath):
        os.makedirs(folderPath) 