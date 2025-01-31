import sys
import os
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from webdriver_manager.chrome import ChromeDriverManager
from datetime import datetime
from win10toast import ToastNotifier  # Notificaciones en Windows

# Agregar la ruta raíz del proyecto para evitar errores de importación
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../..')))

# Importaciones de módulos internos
from app.modules.scraper import obtener_datos_scraping
from app.modules.csv_handler import cargar_datos_en_csv, cargar_datos_en_google_sheets
from app.auth.login_handler import login  # Asegurar que 'auth' está dentro de 'app'


def mostrar_notificacion(titulo, mensaje):
    """Muestra una notificación automática que se cierra después de 5 segundos."""
    toaster = ToastNotifier()
    toaster.show_toast(titulo, mensaje, duration=5, threaded=True)


def ejecutar_proceso():
    mostrar_notificacion("Inicio del Proceso", "El programa ha comenzado a ejecutarse.")

    # Configuración de Chrome en modo headless
    chrome_options = Options()
    # chrome_options.add_argument("--headless")  
    chrome_options.add_argument("--disable-gpu")  # Desactiva el uso de GPU
    chrome_options.add_argument("--no-sandbox")  # Para evitar problemas en sistemas Linux
    chrome_options.add_argument("--disable-dev-shm-usage")  # Usar menos memoria compartida
    chrome_options.add_argument("--log-level=3")  # Nivel de logs bajo
    chrome_options.add_experimental_option("excludeSwitches", ["enable-logging"])  # Desactiva logs innecesarios

    # Inicializar WebDriver
    driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=chrome_options)

    try:
        # Ejecutar scraping y obtener datos
        df_total, fecha_actual = obtener_datos_scraping(driver)

        if df_total is not None and not df_total.empty:
            print("Guardando datos en CSV...")
            cargar_datos_en_csv(df_total, fecha_actual)

            print("Subiendo datos a Google Sheets...")
            cargar_datos_en_google_sheets(df_total, fecha_actual)

        else:
            print("No se generaron datos válidos. Terminando proceso.")

    except Exception as e:
        print(f"❌ Error durante la ejecución: {e}")

    finally:
        driver.quit()  # Cerrar WebDriver al finalizar

    mostrar_notificacion("Proceso Completo", "El programa ha finalizado correctamente.")


if __name__ == "__main__":
    ejecutar_proceso()
