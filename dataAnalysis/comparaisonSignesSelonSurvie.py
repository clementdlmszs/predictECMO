import pandas as pd
import numpy as np
from tqdm import tqdm
import matplotlib.pyplot as plt
import datetime

dataPath = "../data/"

patients_df = pd.read_parquet(dataPath + "patients.parquet")

nb_patients = len(patients_df)


def allValuesAndDates(variableStr, maxTime):

    arrayListValues_Survived = []
    arrayListTimes_Survived = []

    arrayListValues_Deceased = []
    arrayListTimes_Deceased = []

    for index, row in tqdm(patients_df.iterrows(), total=nb_patients):

        encounterId = str(row["encounterId"])
        dureeSejour = (row["withdrawal_date"] - row["installation_date"]).total_seconds() / 3600

        if dureeSejour > 0:
            dfPath = dataPath + encounterId + "/" + variableStr + ".parquet"
            df = pd.read_parquet(dfPath)
            data_np = df.to_numpy()

            values = data_np[:,0]
            times = data_np[:,1]

            isDeceased = pd.read_parquet(dataPath + encounterId + "/Death.parquet").to_numpy()[0][0]

            if isDeceased:
                arrayListValues_Deceased.append(values)
                arrayListTimes_Deceased.append(times)
            else:
                arrayListValues_Survived.append(values)
                arrayListTimes_Survived.append(times)

    tabValues_Survived = np.concatenate(arrayListValues_Survived)
    tabTimes_Survived = np.concatenate(arrayListTimes_Survived)

    tabValues_Deceased = np.concatenate(arrayListValues_Deceased)
    tabTimes_Deceased = np.concatenate(arrayListTimes_Deceased)

    return tabValues_Survived, tabTimes_Survived, tabValues_Deceased, tabTimes_Deceased


def listValuesHourly(values, times, maxTime):
    
    listValuesHourly = [[] for _ in range(maxTime)]

    for i in range(np.size(times)):
        hour = int(times[i] // 60)
        if hour < maxTime:
            listValuesHourly[hour].append(values[i])
    
    return listValuesHourly


def meanHourly(listValuesHourly, maxTime):
    arrayMeanHourly = np.zeros(shape=(maxTime))

    for i in range(maxTime):
        arrayValues_i = np.array(listValuesHourly[i])
        arrayValues_i = arrayValues_i.astype(float)
        mean_i = np.nanmean(arrayValues_i)
        arrayMeanHourly[i] = mean_i
    
    return arrayMeanHourly


def plotGraph(axs, axs_x, axs_y, variableStr, maxTime, y_label_graph, title_graph):
    
    values_Survived, times_Survived, values_Deceased, times_Deceased = allValuesAndDates(variableStr, maxTime)

    listValuesHourly_Survived = listValuesHourly(values_Survived, times_Survived, maxTime)
    listValuesHourly_Deceased = listValuesHourly(values_Deceased, times_Deceased, maxTime)

    meanHourly_Survived = meanHourly(listValuesHourly_Survived, maxTime)
    meanHourly_Deceased = meanHourly(listValuesHourly_Deceased, maxTime)
    
    if variableStr == "DebitEcmo" or variableStr == "Diurese":
        for i in range(0,maxTime,2):
            meanHourly_Survived[i], meanHourly_Survived[i+1] = (meanHourly_Survived[i] + meanHourly_Survived[i+1])*0.5, (meanHourly_Survived[i] + meanHourly_Survived[i+1])*0.5
            meanHourly_Deceased[i], meanHourly_Deceased[i+1] = (meanHourly_Deceased[i] + meanHourly_Deceased[i+1])*0.5, (meanHourly_Deceased[i] + meanHourly_Deceased[i+1])*0.5


    axs[axs_x][axs_y].plot(range(0,maxTime), meanHourly_Survived, '.b-', label="Survived")
    axs[axs_x][axs_y].plot(range(0,maxTime), meanHourly_Deceased, '.k-', label="Deceased")
    axs[axs_x][axs_y].set_xlabel('Time from admission (hours)')
    axs[axs_x][axs_y].set_ylabel(y_label_graph)
    axs[axs_x][axs_y].set_title('Mean hourly evolution of the ' + title_graph)
    axs[axs_x][axs_y].legend()


maxTime = 168

fig, axs = plt.subplots(3, 3, figsize=(20, 14))

plotGraph(axs, 0, 0, "HR", maxTime, "Heart Rate (pulse/minute)", "Heart Rate")
plotGraph(axs, 0, 1, "Temperature", maxTime, "Temperature (°C)", "Temperature")
plotGraph(axs, 0, 2, "SpO2", maxTime, "SpO2 (%)", "SpO2")
plotGraph(axs, 1, 0, "PAD_I", maxTime, "Invasive DIASTOLIC BP (mmHg)", "Invasive DIASTOLIC Blood Pressure")
# plotGraph(axs, 2, 0, "PAD_NI", maxTime, "Non Invasive DIASTOLIC BP (mmHg)", "Non Invasive DIASTOLIC Blood Pressure")
plotGraph(axs, 1, 1, "PAS_I", maxTime, "Invasive SYSTOLIC BP (mmHg)", "Invasive SYSTOLIC Blood Pressure")
# plotGraph(axs, 2, 1, "PAS_NI", maxTime, "Non Invasive SYSTOLIC BP (mmHg)", "Non Invasive SYSTOLIC Blood Pressure")
plotGraph(axs, 1, 2, "PAM_I", maxTime, "Invasive AVERAGE BP (mmHg)", "Invasive AVERAGE Blood Pressure")
# plotGraph(axs, 2, 2, "PAM_NI", maxTime, "Non Invasive AVERAGE BP (mmHg)", "Non Invasive AVERAGE Blood Pressure")
plotGraph(axs, 2, 0, "DebitEcmo", maxTime, "ECMO Debit (l/minute)", "ECMO Debit")
plotGraph(axs, 2, 1, "RR", maxTime, "Respiratory Rate (breaths/minute)", "Respiratory Rate")
plotGraph(axs, 2, 2, "Diurese", maxTime, "Diuresis (mL)", "Diuresis")

# Adjust layout
plt.tight_layout()

# Show plot
plt.show()