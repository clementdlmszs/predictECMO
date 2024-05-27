import pandas as pd
import numpy as np
from tqdm import tqdm
import matplotlib.pyplot as plt
import pyarrow.parquet as pq
import pyarrow as pa
from datetime import datetime

from gestionDonneesAberrantes import *
from moyennage import *
from gestionValeursManquantes import *
from exportStats import *
from regroupement import *


def gestionDonneesAberrantesFull(dataGroup):

    gestionDonneesAberrantes(dataGroup, "HR", 20, 200, 'HR')
    gestionDonneesAberrantes(dataGroup, "SpO2", 50, 100, 'SpO2')
    gestionDonneesAberrantes(dataGroup, "PAD_I", 20, 130, 'pad_i')
    gestionDonneesAberrantes(dataGroup, "PAD_NI", 20, 130, 'pad_ni')
    gestionDonneesAberrantes(dataGroup, "PAM_I", 30, 200, 'pam_i')
    gestionDonneesAberrantes(dataGroup, "PAM_NI", 30, 200, 'pam_ni')
    gestionDonneesAberrantes(dataGroup, "PAS_I", 40, 230, 'pas_i')
    gestionDonneesAberrantes(dataGroup, "PAS_NI", 40, 230, 'pas_ni')
    gestionDonneesAberrantes(dataGroup, "RR", 5, 50, 'RR')
    gestionDonneesAberrantes(dataGroup, "Temperature", 32, 41, 'temperature')
    gestionDonneesAberrantes(dataGroup, "Diurese", 0, 2,'diurese_heure')
    gestionDonneesAberrantes(dataGroup, "Weight", 30, 300, 'weight')
    gestionDonneesAberrantes(dataGroup, "Weight2", 30, 300, 'weight')
    gestionDonneesAberrantes(dataGroup, "Height", 120, 230, 'height')
    gestionDonneesAberrantes(dataGroup, "FiO2", 20, 100, 'FiO2')
    
    if dataGroup == "dataECMO":
        gestionDonneesAberrantes(dataGroup, "DebitECMO", 0, 8, 'debit')


def moyennageFull(dataGroup):

    gestionIMC(dataGroup)
    gestionDiurese(dataGroup, 6)
    moyenne_sur_x_minutes(dataGroup, "HR", 60, "HR")
    moyenne_sur_x_minutes(dataGroup, "SpO2", 60, "SpO2")
    moyenne_sur_x_minutes(dataGroup, "PAD_I", 60, "pad_i")
    moyenne_sur_x_minutes(dataGroup, "PAM_I", 60, "pam_i")
    moyenne_sur_x_minutes(dataGroup, "PAS_I", 60, "pas_i")
    moyenne_sur_x_minutes(dataGroup, "RR", 60, "RR")
    moyenne_sur_x_minutes(dataGroup, "Temperature", 60, "temperature")
    moyenne_sur_x_minutes(dataGroup, "PAD_NI", 60, 'pad_ni')
    moyenne_sur_x_minutes(dataGroup, "PAM_NI", 60, 'pam_ni')
    moyenne_sur_x_minutes(dataGroup, "PAS_NI", 60, 'pas_ni')
    moyenne_sur_x_minutes(dataGroup, "PAS_NI", 60, 'pas_ni')
    gestionFiO2(dataGroup, 60)

    moyenne_sur_x_minutes(dataGroup, "FiO2", 60, 'FiO2') # Facultatif

    if dataGroup == "dataECMO":
        moyenne_sur_x_minutes(dataGroup, "DebitECMO", 60, "debit")


def gestionValeursManquantesFull(dataGroup):

    gestionValeursManquantes(dataGroup, "HR", "HR", 85)
    gestionValeursManquantes(dataGroup, "SpO2", "SpO2", 96)
    gestionValeursManquantesPA(dataGroup, "PAD", "pad", 60)
    gestionValeursManquantesPA(dataGroup, "PAM", "pam", 80)
    gestionValeursManquantesPA(dataGroup, "PAS", "pas", 125)
    gestionValeursManquantes(dataGroup, "RR", "RR", 22)
    gestionValeursManquantes(dataGroup, "Temperature", "temperature", 37)
    gestionValeursManquantes(dataGroup, "Diurese", "diurese", 0.0015)
    gestionValeursManquantes(dataGroup, "SpO2_sur_FiO2", 'SpO2_sur_FiO2', 2.5)

    gestionValeursManquantes(dataGroup, "FiO2", "FiO2", 40) # Facultatif

    if dataGroup == "dataECMO":
        gestionValeursManquantes(dataGroup, "DebitECMO", "debit", 3)


def exportStatsFull(dataGroup):
    exportStats(dataGroup)


def regroupementFull(dataGroup):
    regroupement(dataGroup)
    regroupement_mask(dataGroup)
    regroupement_static(dataGroup)


dataGroup = "dataECMO"
# dataGroup = "dataRea"

gestionDonneesAberrantesFull(dataGroup)
moyennageFull(dataGroup)
gestionValeursManquantesFull(dataGroup)
exportStatsFull(dataGroup)
regroupementFull(dataGroup)