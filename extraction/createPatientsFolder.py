import pandas as pd
import os

patients_df = pd.read_parquet("data/patients.parquet")

for encounterId in patients_df["encounterId"]:

    folderPath = "data/"+str(encounterId)

    if not os.path.exists(folderPath):
        os.makedirs(folderPath)