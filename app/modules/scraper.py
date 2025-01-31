from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from datetime import datetime, timedelta
import pandas as pd
import pytz
from app.jobs.jobs import jobnames
from selenium import webdriver
from selenium.webdriver.chrome.options import Options

# Establecer la zona horaria de Perú (GMT-5)
zona_peru = pytz.timezone('America/Lima')

# Lógica para realizar el scraping
def obtener_datos_scraping(driver, usar_fecha_fija=False):
    if usar_fecha_fija:
        # Fecha fija de ejecución desde el 1 hasta el 31 de enero de 2025
        fecha_actual = datetime(year=2025, month=1, day=30)  # Fecha fija asignada
        fecha_inicio = (fecha_actual - timedelta(days=5)).strftime("%m/%d/%Y")  # Restar 5 días para fecha de inicio
        fecha_fin = fecha_actual.strftime("%Y/%m/%d")  # Fecha final en YYYY/MM/DD
    else:
        # Fecha actual con la zona horaria de Perú
        fecha_actual = datetime.now()  # Toma la fecha exacta del sistema
        
        fecha_fin = fecha_actual.strftime("%Y/%m/%d")  # Fecha actual en YYYY/MM/DD
        fecha_inicio = (fecha_actual - timedelta(days=5)).strftime("%m/%d/%Y")  # 5 días antes en MM/DD/YYYY

    df_total = pd.DataFrame()

    for job in jobnames:
        driver.get("http://172.30.9.229:8080/scheduling/ejecucionesStatus")
        try:
            print(f"Rango de fechas: Inicio - {fecha_inicio}, Fin - {fecha_fin}")
            WebDriverWait(driver, 20).until(EC.presence_of_element_located((By.ID, "jobname"))).send_keys(job)
            WebDriverWait(driver, 20).until(EC.presence_of_element_located((By.ID, "txtFromDate"))).send_keys(fecha_inicio)
            txt_to_date = WebDriverWait(driver, 20).until(EC.presence_of_element_located((By.ID, "txtToDate")))
            txt_to_date.send_keys(fecha_fin)
            WebDriverWait(driver, 20).until(EC.element_to_be_clickable((By.ID, "Consultar"))).click()
            WebDriverWait(driver, 30).until(EC.presence_of_element_located((By.ID, "tblEjec")))
            headers = [header.text for header in driver.find_elements(By.CSS_SELECTOR, "#tblEjec th.header")]
            rows = [
                [cell.text for cell in row.find_elements(By.TAG_NAME, "td")]
                for row in driver.find_elements(By.CSS_SELECTOR, "#tblEjec tbody tr")
            ]
            if rows:
                df = pd.DataFrame(rows, columns=headers)
                df_total = pd.concat([df_total, df], ignore_index=True)
            else:
                print(f"No hay datos para el job {job}. Continuando...")
        except Exception as e:
            print(f"Error al procesar el job {job}: {e}")
    
    if not df_total.empty:
        for col in df_total.columns:
            if 'fecha' in col.lower():
                df_total[col] = pd.to_datetime(df_total[col], errors='coerce')
                df_total[col] = df_total[col].dt.tz_localize(zona_peru)
                df_total[col] = df_total[col].dt.strftime('%d-%m-%Y %H:%M:%S GMT%z').replace('GMT-0500', 'GMT+5.0')

    return df_total, fecha_actual
