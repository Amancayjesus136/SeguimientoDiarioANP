import sys
import os
import pandas as pd
from io import StringIO
##Time: Utilizada para manipular las fechas y horas
import time
##Utilizada para obtener y cambiar la zona horaria, pues la fuente tiene horario de Mexico.
import pytz
##Utilizadas para manipular las fechas y horas, y darles el formato adecuado
from datetime import date 
from datetime import datetime
from datetime import timedelta

##Selenium: Utilizada para automatizar la navegación a la fuente de datos y la extracción de los mismos 
from selenium import webdriver
##WebDriver: Utilizada para interactuar con un navegador
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
## Update 05/05/2023 Libreria de prueba
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC



## convert_timezone: 
## ****************
## Utiliza para convertir una fecha de zona "timezone_in" a una fecha de zona "timezone_out" en formato String.
## Los valores por defecto de la zona horaria de entrada y de salida son Ciudad de Mexico y Lima respectivamente pero se pueden modificar según necesidad.
def convert_timezone(date, formato_in, formato, timezone_in='America/Mexico_City', timezone_out='America/Lima'):
    date_i = datetime.strptime(date, formato_in)
    tz_in = pytz.timezone(timezone_in)
    date_o = tz_in.localize(date_i)
    tz_out = pytz.timezone(timezone_out)
    date_out = date_o.astimezone(tz_out)
    date_out_str = date_out.strftime(formato)
    return date_out_str


## odate
## ****************
## Nos ayuda a obtener las fechas en los formatos necesarios
def obtener_fechas ():
    hoy = date.today()
    #hoy = datetime(year=2024, month=4, day=27)

    flag = False
    try:
        with open(file='D:/SeguimientoDiarioANP/parametria/fecha_forzada.dat', mode='r', encoding='UTF-8') as lineas:
            for linea in lineas:
                pass
    except:
        print("Error de archivo de fecha, modo automatico")
        linea = ""
    if len(linea) == 8: # Formato ddmmyyyy debe tener 8 caracteres
        fecini = ""
        inicio = linea
        if hoy.month == int(linea[2:4]) and hoy.year == int(linea[4:8]):
            fin = linea[:2]
        elif hoy.month != int(linea[2:4]) and hoy.year == int(linea[4:8]):
            fin = linea[:4]
        else:
            fin = linea
        fecha_ejec = linea[4:8]+linea[2:4]+linea[:2]
        print(f"Ejecucion forzada")
        print(f"Inicio    {inicio}")
        print(f"Fin       {fin}")
        print(f"Ejecucion {fecha_ejec}")
        flag = True
    else:
        d_semana = hoy.weekday()
        if d_semana == 0:
            fecpro = hoy - timedelta(days=3)
            fecini = hoy - timedelta(days=2)
        elif d_semana == 6:
            fecpro = hoy - timedelta(days=2)
            fecini = hoy - timedelta(days=1)    
        else:
            fecpro = hoy - timedelta(days=1)
            fecini = hoy
        print(f'Fecha proceso preliminar: {fecpro}, fecha ejecución preliminar {fecini}')
        es_feriado = filtraFeriado(fecpro)
        if es_feriado == True:
            fecpro = fecpro - timedelta(days=1)
            fecini = fecpro + timedelta(days=1)
        print(f'Fecha proceso real: {fecpro}, fecha ejecución real {fecini}')
        inicio = fecini.strftime("%d%m%Y")
        fin = fecini.strftime("%d")
        fecha_ejec = fecini.strftime("%Y%m%d")
    return fecini, inicio, fin, fecha_ejec, flag




## filtraFeriado
## ****************
## Se empleó para obtener fechas de ejecución, en caso el día haya sido feriado
## NOTA: Se eliminará en una siguiente versión ya que debe tener mantenimiento
def filtraFeriado(fecpro):
  d_dia = fecpro.day
  d_mes = fecpro.month
  d_es_feriado = False
  feriados = [
                  {'month':12, 'date':25},#Navidad
                  {'month': 12, 'date': 8}, #Inmaculada Concepción
                  {'month': 11, 'date': 1}, #Todos los Santos
                  {'month': 10, 'date': 8}, #Combate de Angamos
                  {'month': 8, 'date': 30}, #Santa Rosa de Lima
                  {'month': 7, 'date': 29}, #Fiestas Patrias
                  {'month': 7, 'date': 28}, #Fiestas Patrias
                  {'month': 6, 'date': 29}, #Día de San Pedro y San Pablo
                  {'month': 5, 'date': 1},  #Día del Trabajo
                  {'month': 3, 'date': 29}, #Viernes Santo
                  {'month': 3, 'date': 28},  #Jueves Santo
                  {'month': 1, 'date': 1} #Primero de Enero
             ]
  for i in feriados:  
    if i['month'] == d_mes and i['date'] == d_dia:
      print (f'{d_dia} de {d_mes} Es feriado')
      d_es_feriado = True
      break
  return d_es_feriado



## leer_filtro_job
## ****************
## Lee los archivos que contienen los jobs a considerar dentro de la malla de procesos
def leer_filtro_job(archivo):
    lista = []
    try:
        with open(file=archivo, mode="r", encoding="UTF-8") as file:
            for job in file:
                format_job = job.rstrip("\n")
                if len(job) > 0:
                    lista.append(format_job)
    except Exception as e:
       print(f"Error leyendo archivo {archivo}: {e}", log_name)
    return len(lista), lista



## obtener_folder   
## *****************
## Utilizada para extraer la data de la ruta scheduling para una malla específica y aplicando un filtro a partir de los archivos "malla****.dat"
def obtener_folder(driver, foldername="CR-PEBILDIA-T02", filtro_job = ["PBILCP0001","PBILRP0001","PBILCP0017","PBILCP0053"], filtro_cancelados = ["NOTOK"], inicio_periodo="ddmmyyyy", fin_periodo="dd"):
    objeto_chrome = conn_chrome(driver)
## Se obtiene la URL de la fuente de datos que se encuentra en la red interna. Adicionamos un tiempo de espera de 1 segundo para cualquier demora de conexión.
    objeto_chrome.get("http://150.100.216.64:8080/scheduling/ejecucionesStatus")
## Se ingresan los parámetros de búsqueda de información
## Se buscan las ejecuciones de todos (%) los jobs de la malla o folder (CR-PEBILDIA-T02) dentro del periodo |inicio_periodo --> fin_periodo|
    try:
        objeto_chrome.find_element(By.ID, "jobname").send_keys("%")
        time.sleep(1)
        objeto_chrome.find_element(By.ID, "txtFromDate").send_keys(inicio_periodo)
        time.sleep(1)
        objeto_chrome.find_element(By.ID, "txtToDate").send_keys(fin_periodo)
        time.sleep(1)
        objeto_chrome.find_element(By.ID, "foldername").send_keys(foldername)
        time.sleep(1)
        ##Se hace clic en el botón consultar
        boton_consultar = objeto_chrome.find_element(By.ID, "Consultar")
        objeto_chrome.execute_script('arguments[0].click();', boton_consultar)
        time.sleep(10)
        html_content_Padre = objeto_chrome.find_element(By.ID,"tblEjec")
        html_content = StringIO(html_content_Padre.find_element(By.TAG_NAME,"tbody").text)
        # Nombres de las columnas
        nombres_columnas = ["NO.", 'JOBNAME', "SCHEDTABLE", "APPLICATION", "SUB-APPLICATION", "RUN AS", "ORDER ID", "ODATE", 'START DATE','START TIME', 'END DATE', 'END TIME', "RUN TIME", "RUN COUNTER", "ENDED STATUS", "HOST", "CPUTIME"]
        # Crear un DataFrame de Pandas con nombres de columnas
        df_folder_completo = pd.read_csv(html_content, sep=' ', header=None) 
        df_folder_completo.columns = nombres_columnas

        # Columnas que se van a usar
        df_folder = df_folder_completo.filter(items=['NO.','JOBNAME', 'ODATE', 'START DATE','START TIME', 'END DATE', 'END TIME', 'RUN COUNTER', 'ENDED STATUS'])
        
        # Lectura de los jobs de la malla diaria
        #num_malla, list_jobs_malla = leer_filtro_job("D:/SeguimientoDiarioANP/parametria/malla_pbil.dat")
        list_jobs_malla = pd.read_csv("D:/SeguimientoDiarioANP/parametria/malla_pbil.dat", sep=',')

        # 1° filtro: filtrar los jobs ejecutados en la pagina pero que solo sean de la malla diaria anp
        #df_filtro_folder_malla = df_folder[df_folder['JOBNAME'].isin(list_jobs_malla)].sort_values('NO.')
        #Cruce de los jobs que se han ejecutado
        df_filtro_folder_malla = df_folder.merge(list_jobs_malla, how='inner', on='JOBNAME', indicator=True).drop(columns=['_merge'])
        #Variable para conocer cuantos jobs se han ejecutado correctamente
        #count = len(df_jobs_filtrados.index)
        count_df_malla_anp_ok = len(df_filtro_folder_malla[df_filtro_folder_malla['ENDED STATUS'].isin(["OK"])].index)

        if (not df_filtro_folder_malla.empty) :
            # Convertir las columnas de hora a tipo timedelta
            df_filtro_folder_malla['START_TIME_X'] = pd.to_timedelta(df_filtro_folder_malla['START TIME'])
            df_filtro_folder_malla['END_TIME_X'] = pd.to_timedelta(df_filtro_folder_malla['END TIME'])
            # Calcular la diferencia de tiempo en segundos
            df_filtro_folder_malla['RUN TIME'] = (df_filtro_folder_malla['END_TIME_X'] - df_filtro_folder_malla['START_TIME_X']).dt.total_seconds()
            df_filtro_folder_malla[['RUN TIME']] = df_filtro_folder_malla[['RUN TIME']].astype('int64')
            df_filtro_folder_malla = df_filtro_folder_malla.drop(columns=['START_TIME_X', 'END_TIME_X'])

            # 2° filtro: filtrar los jobs a considerar en el correo
            # punto 1: job inicio, job fin
            job_inicio_fin = ["PBILCP0001","PBILRP0001"]
            df_jobs_inicio_fin = df_filtro_folder_malla[df_filtro_folder_malla['JOBNAME'].isin(job_inicio_fin)]
            df_jobs_inicio_fin_ok = df_jobs_inicio_fin[df_jobs_inicio_fin['ENDED STATUS'].isin(["OK"])]
            
            # punto 2: jobs en monitoreo (17 y 53) 
            job_monitoreo = ["PBILCP0017","PBILCP0053"]
            df_jobs_monitoreo = df_filtro_folder_malla[df_filtro_folder_malla['JOBNAME'].isin(job_monitoreo)]

            # punto 3: jobs cancelados
            df_jobs_notok = df_filtro_folder_malla[df_filtro_folder_malla['ENDED STATUS'].isin(filtro_cancelados)]
            
            # punto 4 extra: union de los jobs monitoreados y cancelados
            df_jobs_adicionales = pd.concat([df_jobs_monitoreo, df_jobs_notok]).drop_duplicates(subset='NO.')
            
            # punto 5: ultimo job en ejecutarse
            df_ult_job_ex = df_filtro_folder_malla.sort_values('NO.').tail(1)

            # punto 6: jobs con exceso de tiempo
            df_job_time_exceso = df_filtro_folder_malla[df_filtro_folder_malla['RUN TIME'] >= df_filtro_folder_malla['TIEMPO_ALERTA']]
 
            # punto 7: ultimo job ejecutado, pero con el estado OK
            df_ult_job_ex_ok =  df_filtro_folder_malla[df_filtro_folder_malla['ENDED STATUS'].isin(["OK"])].sort_values('NO.').tail(1)


            df_jobs_inicio_fin_ok = df_jobs_inicio_fin[df_jobs_inicio_fin['ENDED STATUS'].isin(["OK"])]


            # Agregar columna con e ID para saber a que cuadro va a pertenecer en el correo
            df_jobs_inicio_fin_ok_new = df_jobs_inicio_fin_ok.assign(ID_TBL = 'A') 
            df_jobs_adicionales_new = df_jobs_adicionales.assign(ID_TBL = 'B')
            df_ult_job_ex_new = df_ult_job_ex.assign(ID_TBL = 'C')
            df_job_time_exceso_new = df_job_time_exceso.assign(ID_TBL = 'D')
            df_ult_job_ex_ok_new = df_ult_job_ex_ok.assign(ID_TBL = 'E')

            # Juntar todos los jobs a subir al form
            df_jobs_filtrados = pd.concat([df_jobs_inicio_fin_ok_new, df_jobs_adicionales_new, df_ult_job_ex_new,df_job_time_exceso_new,df_ult_job_ex_ok_new]).drop(columns=['TIEMPO_ALERTA']).sort_values(['ID_TBL','NO.'])

        else:
            df_jobs_filtrados = pd.DataFrame(columns=['NO.','JOBNAME', 'ODATE','START DATE','START TIME', 'END DATE', 'END TIME', 'RUN COUNTER', 'ENDED STATUS', 'RUN TIME' 'ID_TBL'])

    except Exception as e:
        df_jobs_filtrados = pd.DataFrame(columns=['NO.','JOBNAME', 'ODATE','START DATE','START TIME', 'END DATE', 'END TIME', 'RUN COUNTER', 'ENDED STATUS', 'RUN TIME' 'ID_TBL'])
        print(e)
    finally:
        count = len(df_jobs_filtrados.index)
        print(f"Los jobs finales de la malla PBILDIA son {count}")
        print(df_jobs_filtrados)
        objeto_chrome.quit()
    return count, df_jobs_filtrados, count_df_malla_anp_ok


## registrar_form: 
## ***************
## Este método crea un objeto de Google Chrome que accede a la dirección de un Formulario de Google que permite ingresar los datos extraídos.
def registrar_form(form, jobname, odate, inicio, fin, estado, time_duracion, run_counter, id_tbl, count_malla_ok):
    ## Hacemos un bloque Try para manejar los errores, es preferible personalizar un método y utilizar un bloque With, tentativamente para la version 2.0
    try:
        ##Se obtiene la URL del formulario de google. Adicionamos un tiempo de espera de 3 segundos para cualquier demora de conexión.
        form.get("https://docs.google.com/forms/d/e/1FAIpQLSegfGVEX8HAPPThnvXJ8lCEZoDKOLBV00DXqDbG1wv2BjLY2Q/viewform?usp=sf_link")
        time.sleep(2)                

        try:
            print('=====ELEMENTO DEL FORMULARIO=====')
            elementoForm = WebDriverWait(form, 100).until(
                EC.presence_of_element_located((By.XPATH,'//*[@id="mG61Hd"]/div[2]/div/div[1]/div/div[2]/div[1]/div'))
            )
            print("El elemento está presente en el FORMULARIO.")
        except Exception as e:
            print("El elemento no apareció en el FORMULARIO en el tiempo especificado.")

        #time.sleep(2)
        ##Se ubica la ruta de cada elemento HTML de entrada de datos del form. Si se desea agregar un campo, o se edita el formulario revisar el manual.
        ##Campo nombre del job
        input_job = form.find_elements(By.XPATH,'//*[@id="mG61Hd"]/div[2]/div/div[2]/div[1]/div/div/div[2]/div/div[1]/div/div[1]/input')
        ##Campo odate
        input_odate = form.find_elements(By.XPATH, '//*[@id="mG61Hd"]/div[2]/div/div[2]/div[2]/div/div/div[2]/div/div/div[2]/div[1]/div/div[1]/input')
        ##Campo usado como código (nombre de job + odate)
        input_codigo = form.find_elements(By.XPATH, '//*[@id="mG61Hd"]/div[2]/div/div[2]/div[3]/div/div/div[2]/div/div[1]/div/div[1]/input')
        ##Campo de la hora para el hito de inicio del job(Las horas y minutos se encuentras en elementos HTML diferentes)
        inp_hora_inicio = form.find_elements(By.XPATH, '//*[@id="mG61Hd"]/div[2]/div/div[2]/div[4]/div/div/div[2]/div/div[1]/div/div[1]/input')
        ##Campo de la hora para el hito de fin del job
        inp_hora_fin = form.find_elements(By.XPATH,'//*[@id="mG61Hd"]/div[2]/div/div[2]/div[5]/div/div/div[2]/div/div[1]/div/div[1]/input')
        ##Campo estado de la ejecución del job (Puede ser OK, NOT OK ó RUNNING)
        input_estado = form.find_elements(By.XPATH,'//*[@id="mG61Hd"]/div[2]/div/div[2]/div[6]/div/div/div[2]/div/div[1]/div/div[1]/input')
        ##Campo run counter
        input_run_counter = form.find_elements(By.XPATH,'//*[@id="mG61Hd"]/div[2]/div/div[2]/div[7]/div/div/div[2]/div/div[1]/div/div[1]/input')
        ##Campo que me identifica a que tabla del correo a a ir
        input_id_tbl = form.find_elements(By.XPATH, '//*[@id="mG61Hd"]/div[2]/div/div[2]/div[8]/div/div/div[2]/div/div[1]/div/div[1]/input')                              
        ##Campopara saber cuantos jobs se han ejecutado ok, para saber el % de avance
        input_count_malla_ok = form.find_elements(By.XPATH, '//*[@id="mG61Hd"]/div[2]/div/div[2]/div[9]/div/div/div[2]/div/div[1]/div/div[1]/input')
        ##Campo para saber la duracion del job
        input_time_duracion = form.find_elements(By.XPATH, '//*[@id="mG61Hd"]/div[2]/div/div[2]/div[10]/div/div/div[2]/div/div[1]/div/div[1]/input')
        
        ##Boton para enviar el formulario
        boton_enviar = form.find_elements(By.XPATH, '//*[@id="mG61Hd"]/div[2]/div/div[3]/div[1]/div[1]/div/span/span')

        if jobname != "" and odate != "":
            input_job[0].send_keys(f'{jobname}{odate}')
        if jobname != "" and odate != "" and estado != "":
            input_codigo[0].send_keys(f'{jobname}{odate}{estado}')
        if odate != "":
            input_odate[0].send_keys(odate)
        if inicio != "":
            inp_hora_inicio[0].send_keys(inicio)
        if fin != "":
            inp_hora_fin[0].send_keys(fin)
        if estado != "":
            input_estado[0].send_keys(estado)
        if run_counter != "":
            input_run_counter[0].send_keys(run_counter)
        if id_tbl != "":
            input_id_tbl[0].send_keys(id_tbl)
        if count_malla_ok != "":
            input_count_malla_ok[0].send_keys(count_malla_ok)
        if time_duracion != "":
            input_time_duracion[0].send_keys(time_duracion)    
        
        boton_enviar[0].click()
        time.sleep(4)
    ##En caso se obtenga algún error se registra en el archivo log_anp.log
    except Exception as e:
        print(e)

## conn_chrome
## **********************
## Se utiliza para crear la conexión con Google Chrome, mediante el drive chrome_driver.exe
## Se reemplazó en la versión 1.4 por la librería WebdriverManager que valida y actualiza dicho driver, sin embargo la red de banco no permite la conexión al repositorio
## de descarga oficial de Google, por este motivo se emplea la descarga manual (De forma mensual) del driver desde https://chromedriver.chromium.org/downloads
def conn_chrome(driver):
    try:             
        driver_install = ChromeDriverManager(path=r".\\driver").install()
        print(driver_install)
    except Exception as e:
        driver_install = ""
    if driver_install == "":
        print("Ejecución Ejecutable Local")
        serv = Service('D:/SeguimientoDiarioANP/driver/chromedriver.exe')
    else:
        serv = Service(driver_install)
    options = webdriver.ChromeOptions()
    options.add_argument('--headless')
    options.add_argument('--no-sandbox')
    options.add_argument('--disable-dev-shm-usage')
    chrome = webdriver.Chrome(service=serv, options=options)
    return chrome


def registrar_jobs(driver, df_completo, count_malla_ok):
    jobname = "XXXXXXXXXX"
    odate = "ddmmyyyy"
    inicio = "00:00:00"
    fin = "00:00:00"
    estado = "OK"

    try:
        form = conn_chrome(driver)
        for _, row in df_completo.iterrows():
            jobname = str(row['JOBNAME'])
            odate = str(row['ODATE'])
            odate_date = datetime.strptime(odate, '%Y%m%d')
            odate = odate_date.strftime("%d%m%Y")
            
            valor_inicio = str(row['START DATE']) + " "+ str(row['START TIME']) 
            hora_ini = convert_timezone(valor_inicio, '%Y-%m-%d %H:%M:%S', '%Y-%m-%d %H:%M:%S')
            hora_inicio = datetime.strptime(hora_ini, '%Y-%m-%d %H:%M:%S')
            inicio = hora_inicio.strftime('%H:%M:%S')
            valor_fin = str(row['END DATE']) + " "+ str(row['END TIME'])
            hora_fini = convert_timezone(valor_fin, '%Y-%m-%d %H:%M:%S', '%Y-%m-%d %H:%M:%S')
            hora_fin = datetime.strptime(hora_fini, '%Y-%m-%d %H:%M:%S')
            fin = hora_fin.strftime("%H:%M:%S")
            estado = str(row['ENDED STATUS'])
            run_counter = str(row['RUN COUNTER'])
            id_tbl = str(row['ID_TBL'])
            time_duracion = str(row['RUN TIME'])
            registrar_form(form, jobname, odate, inicio, fin, estado, time_duracion, run_counter, id_tbl, count_malla_ok)
    except Exception as e:
        print(e)
    finally:
        if form:
            form.quit()
        return odate

def obtener_folder_marcahost(driver, foldername="CR-PESANDIA-T02", inicio_periodo="ddmmyyyy", fin_periodo="dd"):
    objeto_chrome = conn_chrome(driver)
## Se obtiene la URL de la fuente de datos que se encuentra en la red interna. Adicionamos un tiempo de espera de 1 segundo para cualquier demora de conexión.
    objeto_chrome.get("http://150.100.216.64:8080/scheduling/ejecucionesStatus")
## Se ingresan los parámetros de búsqueda de información
## Se buscan las ejecuciones de todos (%) los jobs de la malla o folder (CR-PEBILDIA-T02) dentro del periodo |inicio_periodo --> fin_periodo|
    try:
        objeto_chrome.find_element(By.ID, "jobname").send_keys("%")
        time.sleep(1)
        objeto_chrome.find_element(By.ID, "txtFromDate").send_keys(inicio_periodo)
        time.sleep(1)
        objeto_chrome.find_element(By.ID, "txtToDate").send_keys(fin_periodo)
        time.sleep(1)
        objeto_chrome.find_element(By.ID, "foldername").send_keys(foldername)
        time.sleep(1)
        ##Se hace clic en el botón consultar
        boton_consultar = objeto_chrome.find_element(By.ID, "Consultar")
        objeto_chrome.execute_script('arguments[0].click();', boton_consultar)
        time.sleep(10)
        html_content_Padre = objeto_chrome.find_element(By.ID,"tblEjec")
        html_content = StringIO(html_content_Padre.find_element(By.TAG_NAME,"tbody").text)
        text_content = html_content.getvalue()
        text_content = text_content.replace("SAN-REMOVE (HDFS)-CCR", "SAN-REMOVE(HDFS)-CCR")
        html_content = StringIO(text_content)

        # Nombres de las columnas
        nombres_columnas = ["NO.", 'JOBNAME', "SCHEDTABLE", "APPLICATION", "SUB-APPLICATION", "RUN AS", "ORDER ID", "ODATE", 'START DATE','START TIME', 'END DATE', 'END TIME', "RUN TIME", "RUN COUNTER", "ENDED STATUS", "HOST", "CPUTIME"]
        # Crear un DataFrame de Pandas con nombres de columnas
        df_folder_completo = pd.read_csv(html_content, sep=' ', header=None) 
        df_folder_completo.columns = nombres_columnas

        # Columnas que se van a usar
        df_folder = df_folder_completo.filter(items=['NO.','JOBNAME', 'ODATE', 'START DATE','START TIME', 'END DATE', 'END TIME', 'RUN COUNTER', 'ENDED STATUS'])
        
        # Lectura de los jobs de la malla diaria
        #num_malla, list_jobs_malla = leer_filtro_job("D:/SeguimientoDiarioANP/parametria/malla_pbil.dat")
        list_jobs_malla = pd.read_csv("D:/SeguimientoDiarioANP/parametria/malla_psan.dat", sep=',')

        # 1° filtro: filtrar los jobs ejecutados en la pagina pero que solo sean de la malla diaria anp
        #Cruce de los jobs que se han ejecutado
        df_filtro_folder_malla = df_folder.merge(list_jobs_malla, how='inner', on='JOBNAME', indicator=True).drop(columns=['_merge'])
        # Convertir las columnas de hora a tipo timedelta
        df_jobs_inicio_fin_ok = df_filtro_folder_malla[df_filtro_folder_malla['ENDED STATUS'].isin(["OK"])]
        df_last_psan = df_jobs_inicio_fin_ok[df_jobs_inicio_fin_ok["END TIME"] == df_jobs_inicio_fin_ok["END TIME"].max()].head(1)

    except Exception as e:
        df_last_psan = pd.DataFrame(columns=['NO.','JOBNAME', 'ODATE', 'START DATE','START TIME', 'END DATE', 'END TIME', 'RUN COUNTER', 'ENDED STATUS'])
        print(e)
    finally:
        print('Job Marcahost es:')
        print(df_last_psan)
        objeto_chrome.quit()
    return df_last_psan



def registrar_jobs_marcahost(driver, df_completo):
    jobname = "XXXXXXXXXX"
    odate = "ddmmyyyy"
    inicio = "00:00:00"
    fin = "00:00:00"
    estado = "OK"

    try:
        form = conn_chrome(driver)
        for _, row in df_completo.iterrows():
            jobname = str(row['JOBNAME'])
            odate = str(row['ODATE'])
            odate_date = datetime.strptime(odate, '%Y%m%d')
            odate = odate_date.strftime("%d%m%Y")
            
            valor_inicio = str(row['START DATE']) + " "+ str(row['START TIME']) 
            hora_ini = convert_timezone(valor_inicio, '%Y-%m-%d %H:%M:%S', '%Y-%m-%d %H:%M:%S')
            hora_inicio = datetime.strptime(hora_ini, '%Y-%m-%d %H:%M:%S')
            inicio = hora_inicio.strftime('%H:%M:%S')
            valor_fin = str(row['END DATE']) + " "+ str(row['END TIME'])
            hora_fini = convert_timezone(valor_fin, '%Y-%m-%d %H:%M:%S', '%Y-%m-%d %H:%M:%S')
            hora_fin = datetime.strptime(hora_fini, '%Y-%m-%d %H:%M:%S')
            fin = hora_fin.strftime("%H:%M:%S")
            estado = str(row['ENDED STATUS'])
            registrar_form_marcahost(form, jobname, odate, inicio, fin, estado)
    except Exception as e:
        print(e)
    finally:
        form.quit()


def registrar_form_marcahost(form, jobname, odate, inicio, fin, estado):
    ## Hacemos un bloque Try para manejar los errores, es preferible personalizar un método y utilizar un bloque With, tentativamente para la version 2.0
    try:
        ##Se obtiene la URL del formulario de google. Adicionamos un tiempo de espera de 3 segundos para cualquier demora de conexión.
        form.get("https://docs.google.com/forms/d/e/1FAIpQLSegfGVEX8HAPPThnvXJ8lCEZoDKOLBV00DXqDbG1wv2BjLY2Q/viewform?usp=sf_link")
        time.sleep(2)

        try:
            print('=====ELEMENTO DEL FORMULARIO=====')
            elementoForm = WebDriverWait(form, 100).until(
                EC.presence_of_element_located((By.XPATH,'//*[@id="mG61Hd"]/div[2]/div/div[1]/div/div[2]/div[1]/div'))
            )
            print("El elemento está presente en el FORMULARIO.")
        except Exception as e:
            print("El elemento no apareció en el FORMULARIO en el tiempo especificado.")


        #time.sleep(2)
        ##Se ubica la ruta de cada elemento HTML de entrada de datos del form. Si se desea agregar un campo, o se edita el formulario revisar el manual.
        ##Campo nombre del job
        input_job = form.find_elements(By.XPATH,'//*[@id="mG61Hd"]/div[2]/div/div[2]/div[1]/div/div/div[2]/div/div[1]/div/div[1]/input')
        ##Campo odate
        input_odate = form.find_elements(By.XPATH, '//*[@id="mG61Hd"]/div[2]/div/div[2]/div[2]/div/div/div[2]/div/div/div[2]/div[1]/div/div[1]/input')
        ##Campo usado como código (nombre de job + odate)
        input_codigo = form.find_elements(By.XPATH, '//*[@id="mG61Hd"]/div[2]/div/div[2]/div[3]/div/div/div[2]/div/div[1]/div/div[1]/input')
        ##Campo de la hora para el hito de inicio del job(Las horas y minutos se encuentras en elementos HTML diferentes)
        inp_hora_inicio = form.find_elements(By.XPATH, '//*[@id="mG61Hd"]/div[2]/div/div[2]/div[4]/div/div/div[2]/div/div[1]/div/div[1]/input')
        ##Campo de la hora para el hito de fin del job
        inp_hora_fin = form.find_elements(By.XPATH,'//*[@id="mG61Hd"]/div[2]/div/div[2]/div[5]/div/div/div[2]/div/div[1]/div/div[1]/input')
        ##Campo estado de la ejecución del job (Puede ser OK, NOT OK ó RUNNING)
        input_estado = form.find_elements(By.XPATH,'//*[@id="mG61Hd"]/div[2]/div/div[2]/div[6]/div/div/div[2]/div/div[1]/div/div[1]/input')

        ##Boton para enviar el formulario
        boton_enviar = form.find_elements(By.XPATH, '//*[@id="mG61Hd"]/div[2]/div/div[3]/div[1]/div[1]/div/span/span')

        if jobname != "":
            input_job[0].send_keys(f'{jobname}')
        if odate != "":
            input_codigo[0].send_keys(f'MARCAHOST{odate}')
        if odate != "":
            input_odate[0].send_keys(odate)
        if inicio != "":
            inp_hora_inicio[0].send_keys(inicio)
        if fin != "":
            inp_hora_fin[0].send_keys(fin)
        if estado != "":
            input_estado[0].send_keys(estado)

        boton_enviar[0].click()
        time.sleep(4)
    ##En caso se obtenga algún error se registra en el archivo log_anp.log
    except Exception as e:
        print(e)



driver_install = ""
driver = ""
fec_exec, inicio_periodo, fin_periodo, fecha_exec, flag = obtener_fechas()
log_name = f'D:/SeguimientoDiarioANP/ejecuciones/log_anp_{fecha_exec}.txt'
log_name_marcahost = f'D:/SeguimientoDiarioANP/ejecuciones/log_anp_marcahost_{fecha_exec}.txt'

if not os.path.exists(log_name_marcahost):
    with open(log_name_marcahost, 'w') as archivo_marca:
        archivo_marca.write('NO.,JOBNAME,ODATE,START DATE,START TIME,END DATE,END TIME,ENDED STATUS')
else:
    print("El archivo ya existe.")

try:
    df_psan = obtener_folder_marcahost(driver_install, "CR-PESANDIA-T02", inicio_periodo, fin_periodo)
    # La var df_jobs_forms es donde se almacena solo los registros que se van agregar al forms
    df_jobs_forms_psan = df_psan
    
    if(not df_psan.empty):
        df_ejecutado_antes_psan = pd.read_csv(log_name_marcahost, sep= ',')   
        # Identifico los registro que faltan
        jobs_exec_nuevos_psan = df_psan.merge(df_ejecutado_antes_psan, how='outer', indicator=True).loc[lambda x: x['_merge'] == 'left_only']
        # Elimina la columna _merge, que es solo auxiliar
        jobs_exec_nuevos_psan = jobs_exec_nuevos_psan.drop(columns=['_merge'])
        df_ejecutado_ahora_psan = pd.concat([df_ejecutado_antes_psan, jobs_exec_nuevos_psan])  
        df_ejecutado_ahora_psan.to_csv(log_name_marcahost, sep=',', index=False)  
        df_jobs_forms_psan = jobs_exec_nuevos_psan
        
    else:
        print("no se agrega nada")
    
        
    registrar_jobs_marcahost("",df_jobs_forms_psan)

except Exception as e:
        print(e)
finally:
    print("Finaliza busqueda marcahost!!")


#'NO.','JOBNAME', 'ODATE','START DATE','START TIME', 'END DATE', 'END TIME', 'RUN COUNTER', 'ENDED STATUS', 'RUN TIME' 'ID_TBL'
if not os.path.exists(log_name):
    with open(log_name, 'w') as archivo:
        archivo.write('NO.,JOBNAME,ODATE,START DATE,START TIME,END DATE,END TIME,RUN COUNTER,ENDED STATUS')
else:
    print("El archivo ya existe.")

try:

    filtro_job = ["PBILCP0001","PBILRP0001","PBILCP0017","PBILCP0053"]
    filtro_cancelados = ["NOTOK"]

    count_pbil_ahora, df_pbil, count_malla_ok = obtener_folder(driver_install, "CR-PEBILDIA-T02", filtro_job, filtro_cancelados, inicio_periodo, fin_periodo)
    # La var df_jobs_forms es donde se almacena solo los registros que se van agregar al forms
    df_jobs_forms = df_pbil
    
    if(not df_pbil.empty):
        df_ejecutado_antes = pd.read_csv(log_name, sep= ',')
        cant_pbil_antes = df_ejecutado_antes.shape[0]
        print(f'antes: {cant_pbil_antes} y despues: {count_pbil_ahora}')        
        # Identifico los registro que faltan
        jobs_exec_nuevos = df_pbil.merge(df_ejecutado_antes, how='outer', indicator=True).loc[lambda x: x['_merge'] == 'left_only']
        # Elimina la columna _merge, que es solo auxiliar
        jobs_exec_nuevos = jobs_exec_nuevos.drop(columns=['_merge'])
        df_ejecutado_ahora = pd.concat([df_ejecutado_antes, jobs_exec_nuevos]).sort_values(['ID_TBL','NO.'])    
        df_ejecutado_ahora.to_csv(log_name, sep=',', index=False)
        df_jobs_forms = jobs_exec_nuevos
        
    else:
        print("no se agrega nada")
        
    registrar_jobs("",df_jobs_forms,count_malla_ok)

except Exception as e:
        print(e)
finally:
    print("PROGRAMA FINALIZADO :) !!")










