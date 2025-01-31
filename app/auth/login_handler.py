from pydrive2.auth import GoogleAuth
from pydrive2.drive import GoogleDrive
import os

# Definir la ruta completa a tus credenciales
directorio_crdenciales = r'D:\SeguimientoDiarioANP\app\config\credentials_module.json'

def login():
    gauth = GoogleAuth()
    gauth.LoadCredentialsFile(directorio_crdenciales)

    if gauth.access_token_expired:
        gauth.Refresh()
        gauth.SaveCredentialsFile(directorio_crdenciales)
    else:
        gauth.Authorize()

    return GoogleDrive(gauth)
