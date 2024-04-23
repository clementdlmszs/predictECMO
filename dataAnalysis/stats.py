import pandas as pd
import numpy as np
from tqdm import tqdm
import matplotlib.pyplot as plt


dataPath = "data/"

patients_df = pd.read_parquet(dataPath + "patients.parquet")

nb_patients = len(patients_df)

# Récupère toutes les valeurs de la variable passée en paramètre
# Concatène les valeurs de tous les patients de la bdd
def allValues(variableStr):

    arrayList = []

    for index, row in tqdm(patients_df.iterrows(), total=nb_patients):
        encounterId = str(row["encounterId"])
        dfPath = dataPath + encounterId + "/" + variableStr + ".parquet"
        df = pd.read_parquet(dfPath)
        values = df.to_numpy()[:,0] # On récupère seulement les valeurs, et pas les temps associés
        arrayList.append(values)

    tabValues = np.concatenate(arrayList)

    return tabValues

# Affichage des histogrammes et des boxplots pour une variable passée en paramètre
def plotHistAndBox(axs, axs_x, valuesArray, variableStr):

    values = valuesArray.astype(float)
    values = values[~np.isnan(values)]  # On enlève tous les nan
    
    # Plot histogram
    axs[axs_x][0].hist(values, bins=50, edgecolor='black')
    axs[axs_x][0].set_xlabel('Value')
    axs[axs_x][0].set_ylabel('Frequency')
    axs[axs_x][0].set_title('Histogram for ' + variableStr)

    # Plot boxplot
    axs[axs_x][1].boxplot(values, vert=False)
    axs[axs_x][1].set_xlabel('Value')
    axs[axs_x][1].set_title('Boxplot for ' + variableStr)


nb_graphs = 15
fig, axs = plt.subplots(nb_graphs, 2, figsize=(14, 5*nb_graphs))

debitECMO = allValues("DebitEcmo")
plotHistAndBox(axs, 0, debitECMO, "ECMO Debit")

height = allValues("Height") 
plotHistAndBox(axs, 1, height, "Height")

# hr = allValues("HR")
# plotHistAndBox(axs, 2, hr, "Heart Rate")

# pad_i = allValues("PAD_I")
# plotHistAndBox(axs, 3, pad_i, "Invasive DIASTOLIC Blood Pressure")

# pad_ni = allValues("PAD_NI")
# plotHistAndBox(axs, 4, pad_ni, "Non Invasive DIASTOLIC Blood Pressure")

# pas_i = allValues("PAS_I")
# plotHistAndBox(axs, 5, pas_i, "Invasive SYSTOLIC Blood Pressure")

# pas_ni = allValues("PAS_NI")
# plotHistAndBox(axs, 6, pas_ni, "Non Invasive SYSTOLIC Blood Pressure")

# pam_i = allValues("PAM_I")
# plotHistAndBox(axs, 7, pam_i, "Invasive AVERAGE Blood Pressure")

# pam_ni = allValues("PAM_NI")
# plotHistAndBox(axs, 8, pam_ni, "Non Invasive AVERAGE Blood Pressure")

# rr = allValues("RR")
# plotHistAndBox(axs, 9, rr, "Respiratory Rate")

# spo2 = allValues("SpO2")
# plotHistAndBox(axs, 10, spo2, "SpO2")

# temperature = allValues("Temperature")
# plotHistAndBox(axs, 11, temperature, "Temperature")

# diurese = allValues("Diurese")
# plotHistAndBox(axs, 12, diurese, "Diurese")

# weight1 = allValues("Weight")
# weight2 = allValues("Weight2")
# weight = np.concatenate((weight1,weight2))
# plotHistAndBox(axs, 13, weight, "Weight")

# death = allValues("Death")
# plotHistAndBox(axs, 14, death, "Death")

# Adjust layout
plt.tight_layout()

# Show plot
plt.show()