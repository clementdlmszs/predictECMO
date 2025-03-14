from sqlalchemy import create_engine, text
import pandas as pd
from tqdm import tqdm
from extractionFunctions import *

SERVER = "SVM-ICCA-REP"

DATABASE = 'CISReportingDB'

engine = create_engine('mssql+pymssql://@' + SERVER + '/' + DATABASE)

sqlPath = "extraction/scriptsSQL/"

dataPath = "dataECMO/"
# dataPath = "dataRea/"

filePatients = "patients.parquet"

patients_df = pd.read_parquet(dataPath + filePatients)

dataPath += "rawData/"

with engine.connect() as con:

    for index, row in tqdm(patients_df.iterrows(), total=len(patients_df)):
        
        encounterId = row["encounterId"]
        installation_date = row["installation_date"]
        withdrawal_date = row["withdrawal_date"]

        # extractDebitECMO(con, sqlPath, dataPath, encounterId, installation_date, withdrawal_date)
        # extractHR(con, sqlPath, dataPath, encounterId, installation_date, withdrawal_date)
        # extractPAD_I(con, sqlPath, dataPath, encounterId, installation_date, withdrawal_date)
        # extractPAD_NI(con, sqlPath, dataPath, encounterId, installation_date, withdrawal_date)
        # extractPAM_I(con, sqlPath, dataPath, encounterId, installation_date, withdrawal_date)
        # extractPAM_NI(con, sqlPath, dataPath, encounterId, installation_date, withdrawal_date)
        # extractPAS_I(con, sqlPath, dataPath, encounterId, installation_date, withdrawal_date)
        # extractPAS_NI(con, sqlPath, dataPath, encounterId, installation_date, withdrawal_date)
        # extractRR(con, sqlPath, dataPath, encounterId, installation_date, withdrawal_date)
        # extractSpO2(con, sqlPath, dataPath, encounterId, installation_date, withdrawal_date)
        # extractFiO2(con, sqlPath, dataPath, encounterId, installation_date, withdrawal_date)
        # extractTemperature(con, sqlPath, dataPath, encounterId, installation_date, withdrawal_date)
        # extractWeight(con, sqlPath, dataPath, encounterId, installation_date)
        # extractWeight2(con, sqlPath, dataPath, encounterId, installation_date)
        # extractHeight(con, sqlPath, dataPath, encounterId, installation_date)
        # extractDiurese(con, sqlPath, dataPath, encounterId, installation_date, withdrawal_date)
        extractCompliance(con, sqlPath, dataPath, encounterId, installation_date, withdrawal_date)
        extractVolumeCourant(con, sqlPath, dataPath, encounterId, installation_date, withdrawal_date)
        extractPplat(con, sqlPath, dataPath, encounterId, installation_date, withdrawal_date)
        extractPEP(con, sqlPath, dataPath, encounterId, installation_date, withdrawal_date)
        # extractDeath(con, sqlPath, dataPath, encounterId)
        # extractIGS(con, sqlPath, dataPath, encounterId)

