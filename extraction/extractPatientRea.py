from sqlalchemy import create_engine, text
import pandas as pd

SERVER = "SVM-ICCA-REP"

DATABASE = 'CisReportingActiveDB0'

engine = create_engine('mssql+pymssql://@' + SERVER + '/' + DATABASE)

sqlFile = "extraction/scriptsSQL/exportPatientReaRangueilDepuis2020.sql"
destinationFile = "data/patientsRea.parquet"

with engine.connect() as con:
    with open(sqlFile) as file:
        query = text(file.read())
        
        result = con.execute(query)

        df = pd.DataFrame(result.fetchall(), columns=result.keys())

        df.to_parquet(destinationFile, index=False)