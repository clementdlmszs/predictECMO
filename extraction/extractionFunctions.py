from sqlalchemy import create_engine, text
import pandas as pd


# Fonction générique d'extraction d'une donnée, appelée par chaque fonction spécifique d'extraction
# Les données sont stockées dans des fichiers correspondant au nom de la variable enregistrée
# et dans le dossier associé à l'encounterId correspondant
def genericExtract(con, dataExtracted, sqlPath, dataPath, params, encounterId):
    
    sqlFile = sqlPath + "extract" + dataExtracted + ".sql"
    dataFile = dataPath + str(encounterId) + "/" + dataExtracted + ".parquet"

    with open(sqlFile) as file:
        query = text(file.read())
        
        for param in params:
            param_name = param[0]
            query = query.bindparams(**{param_name: param[1]})

        result = con.execute(query)

        df = pd.DataFrame(result.fetchall(), columns=result.keys())
        df.to_parquet(dataFile, index=False)
    
    return result


def extractDebitECMO(con, sqlPath, dataPath, encounterId, installation_date, withdrawal_date):
    dataExtracted = "DebitEcmo"
    params = [["encounterId",encounterId],["installation_date",installation_date],["withdrawal_date", withdrawal_date]]
    genericExtract(con, dataExtracted, sqlPath, dataPath, params, encounterId)


def extractCOVID(con, sqlPath, dataPath, encounterId):
    dataExtracted = "COVID"
    params = [["encounterId",encounterId]]
    genericExtract(con, dataExtracted, sqlPath, dataPath, params, encounterId)


def extractHR(con, sqlPath, dataPath, encounterId, installation_date, withdrawal_date):
    dataExtracted = "HR"
    params = [["encounterId",encounterId],["installation_date",installation_date],["withdrawal_date", withdrawal_date]]
    genericExtract(con, dataExtracted, sqlPath, dataPath, params, encounterId)


def extractPAD_I(con, sqlPath, dataPath, encounterId, installation_date, withdrawal_date):
    dataExtracted = "PAD_I"
    params = [["encounterId",encounterId],["installation_date",installation_date],["withdrawal_date", withdrawal_date]]
    genericExtract(con, dataExtracted, sqlPath, dataPath, params, encounterId)


def extractPAD_NI(con, sqlPath, dataPath, encounterId, installation_date, withdrawal_date):
    dataExtracted = "PAD_NI"
    params = [["encounterId",encounterId],["installation_date",installation_date],["withdrawal_date", withdrawal_date]]
    genericExtract(con, dataExtracted, sqlPath, dataPath, params, encounterId)


def extractPAM_I(con, sqlPath, dataPath, encounterId, installation_date, withdrawal_date):
    dataExtracted = "PAM_I"
    params = [["encounterId",encounterId],["installation_date",installation_date],["withdrawal_date", withdrawal_date]]
    genericExtract(con, dataExtracted, sqlPath, dataPath, params, encounterId)


def extractPAM_NI(con, sqlPath, dataPath, encounterId, installation_date, withdrawal_date):
    dataExtracted = "PAM_NI"
    params = [["encounterId",encounterId],["installation_date",installation_date],["withdrawal_date", withdrawal_date]]
    genericExtract(con, dataExtracted, sqlPath, dataPath, params, encounterId)


def extractPAS_I(con, sqlPath, dataPath, encounterId, installation_date, withdrawal_date):
    dataExtracted = "PAS_I"
    params = [["encounterId",encounterId],["installation_date",installation_date],["withdrawal_date", withdrawal_date]]
    genericExtract(con, dataExtracted, sqlPath, dataPath, params, encounterId)


def extractPAS_NI(con, sqlPath, dataPath, encounterId, installation_date, withdrawal_date):
    dataExtracted = "PAS_NI"
    params = [["encounterId",encounterId],["installation_date",installation_date],["withdrawal_date", withdrawal_date]]
    genericExtract(con, dataExtracted, sqlPath, dataPath, params, encounterId)


def extractRR(con, sqlPath, dataPath, encounterId, installation_date, withdrawal_date):
    dataExtracted = "RR"
    params = [["encounterId",encounterId],["installation_date",installation_date],["withdrawal_date", withdrawal_date]]
    genericExtract(con, dataExtracted, sqlPath, dataPath, params, encounterId)


def extractSpO2(con, sqlPath, dataPath, encounterId, installation_date, withdrawal_date):
    dataExtracted = "SpO2"
    params = [["encounterId",encounterId],["installation_date",installation_date],["withdrawal_date", withdrawal_date]]
    genericExtract(con, dataExtracted, sqlPath, dataPath, params, encounterId)


def extractTemperature(con, sqlPath, dataPath, encounterId, installation_date, withdrawal_date):
    dataExtracted = "Temperature"
    params = [["encounterId",encounterId],["installation_date",installation_date],["withdrawal_date", withdrawal_date]]
    genericExtract(con, dataExtracted, sqlPath, dataPath, params, encounterId)


def extractWeight(con, sqlPath, dataPath, encounterId, installation_date):
    dataExtracted = "Weight"
    params = [["encounterId",encounterId],["installation_date",installation_date]]
    genericExtract(con, dataExtracted, sqlPath, dataPath, params, encounterId)


def extractWeight2(con, sqlPath, dataPath, encounterId, installation_date):
    dataExtracted = "Weight2"
    params = [["encounterId",encounterId],["installation_date",installation_date]]
    genericExtract(con, dataExtracted, sqlPath, dataPath, params, encounterId)


def extractHeight(con, sqlPath, dataPath, encounterId, installation_date):
    dataExtracted = "Height"
    params = [["encounterId",encounterId],["installation_date",installation_date]]
    genericExtract(con, dataExtracted, sqlPath, dataPath, params, encounterId)


def extractDiurese(con, sqlPath, dataPath, encounterId, installation_date, withdrawal_date):
    dataExtracted = "Diurese"
    params = [["encounterId",encounterId],["installation_date",installation_date],["withdrawal_date", withdrawal_date]]
    genericExtract(con, dataExtracted, sqlPath, dataPath, params, encounterId)


def extractDeath(con, sqlPath, dataPath, encounterId):
    dataExtracted = "Death2"
    params = [["encounterId",encounterId]]
    genericExtract(con, dataExtracted, sqlPath, dataPath, params, encounterId)