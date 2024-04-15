import pandas as pd
import os

patients_df = pd.read_parquet("data/patients.parquet")

# Création d'un dossier pour chaque encounterId différent
for encounterId in patients_df["encounterId"]:

    folderPath = "data/"+str(encounterId)

    if not os.path.exists(folderPath):
        os.makedirs(folderPath)