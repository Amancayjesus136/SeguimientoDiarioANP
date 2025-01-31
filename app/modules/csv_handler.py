import io
import pandas as pd
from pydrive2.drive import GoogleDrive
from datetime import datetime
from app.auth.login_handler import login
from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from app.jobs.groups import job_groups
import gspread

# Función para convertir columnas de tipo datetime a cadenas
def convertir_fechas_a_cadena(df):
    for col in df.columns:
        if pd.api.types.is_datetime64_any_dtype(df[col]):
            df[col] = df[col].dt.strftime('%Y-%m-%d %H:%M:%S')
    return df

# Función para agregar la fecha mínima por cada JOBNAME, Grupo y ODATE
def agregar_fecha_minima(df):
    df['Fecha Mínima'] = df.groupby(['JOBNAME', 'Grupo', 'ODATE'])['Fecha Ejecución'].transform('min')
    return df

# Función para cargar los datos en un archivo CSV directamente en memoria
def cargar_datos_en_csv(df_total, fecha_actual):
    nombre_archivo_csv = f"Jobs_IDA_Segmentos_Diario_IntraMIS_{fecha_actual.strftime('%Y%m')}.csv"
    try:
        # Añadir la fecha de ejecución como columna
        df_total['Fecha Ejecución'] = fecha_actual.strftime('%Y-%m-%d %H:%M:%S')

        # Convertir todas las fechas a cadenas
        df_total = convertir_fechas_a_cadena(df_total)

        # Guardar el archivo CSV en memoria
        csv_buffer = io.StringIO()
        df_total.to_csv(csv_buffer, index=False)
        csv_buffer.seek(0)

        print(f"Se cargaron {len(df_total)} filas exitosamente en memoria en formato CSV.")
        carpeta_drive_1 = '1ibXFWeRn9BwZrVbAmZe2jxnDUr7RVybt'  # ID de la carpeta (CSV)
        actualizar_contenido_en_drive(nombre_archivo_csv, csv_buffer.getvalue(), carpeta_drive_1)

    except Exception as e:
        print(f"Error al cargar los datos en el archivo CSV: {e}")

def cargar_datos_en_google_sheets(df_total, fecha_actual):
    try:
        # Añadir la fecha de ejecución como columna
        df_total['Fecha Ejecución'] = fecha_actual.strftime('%Y-%m-%d %H:%M:%S')

        # Agregar la columna 'Grupo' al DataFrame usando el diccionario importado
        df_total['Grupo'] = df_total['JOBNAME'].map(job_groups)  # 'JOBNAME' debe ser la columna que contiene los nombres de los trabajos

        # Ordenar el DataFrame por 'Grupo', 'ODATE', luego por 'START TIME'
        df_total = df_total.sort_values(by=['Grupo', 'ODATE', 'START TIME'])

        # Agrupar por 'Grupo' y 'ODATE' para calcular la fecha mínima en 'START TIME'
        fecha_minima_por_grupo = df_total.groupby(['Grupo', 'ODATE'])['START TIME'].min().reset_index()
        fecha_minima_por_grupo = fecha_minima_por_grupo.rename(columns={'START TIME': 'Fecha Mínima'})

        # Agrupar por 'Grupo' y 'ODATE' para calcular la fecha máxima en 'END TIME'
        fecha_maxima_por_grupo = df_total.groupby(['Grupo', 'ODATE'])['END TIME'].max().reset_index()
        fecha_maxima_por_grupo = fecha_maxima_por_grupo.rename(columns={'END TIME': 'Fecha Máxima'})

        # Unir el DataFrame original con las fechas mínimas y máximas por grupo y ODATE
        df_total = pd.merge(df_total, fecha_minima_por_grupo[['Grupo', 'ODATE', 'Fecha Mínima']], on=['Grupo', 'ODATE'], how='left')
        df_total = pd.merge(df_total, fecha_maxima_por_grupo[['Grupo', 'ODATE', 'Fecha Máxima']], on=['Grupo', 'ODATE'], how='left')

        # Autenticación con Google Sheets
        drive = login()
        gauth = drive.auth  
        credenciales_archivo = r'D:\SeguimientoDiarioANP\app\config\credentials_module.json'
        gauth.LoadCredentialsFile(credenciales_archivo)
        
        if not gauth.credentials:
            raise Exception("No se encontraron credenciales en el archivo.")
        
        credentials_dict = {
            "token": gauth.credentials.access_token,
            "refresh_token": gauth.credentials.refresh_token,
            "token_uri": gauth.credentials.token_uri,
            "client_id": gauth.credentials.client_id,
            "client_secret": gauth.credentials.client_secret
        }

        creds = Credentials.from_authorized_user_info(credentials_dict)

        if creds.expired and creds.refresh_token:
            creds.refresh(Request())

        gc = gspread.authorize(creds)

        spreadsheet = gc.open_by_key('1APgIza8nUp35HhulqopCqJQAAN56KBg-qVl_Kejr4SQ')
        sheet = spreadsheet.worksheet('Data_recibida')

        # Borrar todo el contenido de la hoja antes de cargar nuevos datos
        sheet.clear()

        # Agregar los nuevos datos (cabecera + registros)
        header = list(df_total.columns)
        sheet.append_row(header, value_input_option='RAW')
        new_data = df_total.values.tolist()
        sheet.append_rows(new_data, value_input_option='RAW')

        print(f"Se han cargado {len(new_data)} registros nuevos en Google Sheets.")

    except Exception as e:
        print(f"Error al cargar los datos en Google Sheets: {e}")

# Función para actualizar el contenido del archivo en Google Drive
def actualizar_contenido_en_drive(nombre_archivo, contenido, id_folder):
    try:
        credenciales = login()
        archivo = buscar_archivo_en_drive(nombre_archivo, id_folder)
        
        if archivo:
            print(f"El archivo {nombre_archivo} ya existe en la carpeta {id_folder}. Reemplazando contenido...")
            archivo.SetContentString(contenido)
            archivo.Upload()
        else:
            print(f"El archivo {nombre_archivo} no existe en la carpeta {id_folder}. Creando uno nuevo...")
            archivo = credenciales.CreateFile({'title': nombre_archivo,
                                               'parents': [{'kind': 'drive#fileLink', 'id': id_folder}]} )
            archivo.SetContentString(contenido)
            archivo.Upload()

    except Exception as e:
        print(f"Error al actualizar el contenido en Google Drive: {e}")

# Función para buscar un archivo en Google Drive
def buscar_archivo_en_drive(nombre_archivo, id_folder):
    try:
        credenciales = login()
        archivo_lista = credenciales.ListFile({'q': f"'{id_folder}' in parents and title='{nombre_archivo}'"}).GetList()
        return archivo_lista[0] if archivo_lista else None
    except Exception as e:
        print(f"Error al buscar el archivo en Google Drive: {e}")
        return None

# Llamada a las funciones principales
fecha_actual = datetime.now()
