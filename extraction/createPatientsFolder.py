import pandas as pd
import os

patients_df = pd.read_csv("data/patients.csv")

for encounterId in patients_df["encounterId"]:

    folderPath = "data/"+str(encounterId)

    if not os.path.exists(folderPath):
        os.makedirs(folderPath)