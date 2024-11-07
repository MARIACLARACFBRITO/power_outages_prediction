from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.dummy import DummyOperator
import pendulum
import pandas as pd
import requests
from requests.auth import HTTPBasicAuth
from io import StringIO
from os.path import join, exists
import os

username = 'fiap_ziolli_matheus'
password = 'jApNUn762i'

# Lista de localizações (latitude e longitude) com os principais bairros de São Paulo
locations = [
    {"nome": "AguaRasa", "latitude": "-23.5586", "longitude": "-46.5636"},
    {"nome": "AltodePinheiros", "latitude": "-23.5715", "longitude": "-46.7017"},
    {"nome": "Anhanguera", "latitude": "-23.4133", "longitude": "-46.7524"},
    {"nome": "Aricanduva", "latitude": "-23.5721", "longitude": "-46.5171"},
    {"nome": "ArturAlvim", "latitude": "-23.5374", "longitude": "-46.5024"},
    {"nome": "BelaVista", "latitude": "-23.5594", "longitude": "-46.6456"},
    {"nome": "Belem", "latitude": "-23.5314", "longitude": "-46.5919"},
    {"nome": "BomRetiro", "latitude": "-23.5291", "longitude": "-46.6345"},
    {"nome": "Bras", "latitude": "-23.5418", "longitude": "-46.6166"},
    {"nome": "Brasilandia", "latitude": "-23.4643", "longitude": "-46.6798"},
    {"nome": "Butanta", "latitude": "-23.5749", "longitude": "-46.7150"},
    {"nome": "Cachoeirinha", "latitude": "-23.4781", "longitude": "-46.6610"},
    {"nome": "Cambuci", "latitude": "-23.5673", "longitude": "-46.6243"},
    {"nome": "CampoBelo", "latitude": "-23.6245", "longitude": "-46.6655"},
    {"nome": "CampoGrande", "latitude": "-23.6803", "longitude": "-46.6850"},
    {"nome": "CampoLimpo", "latitude": "-23.6406", "longitude": "-46.7634"},
    {"nome": "CapãoRedondo", "latitude": "-23.6565", "longitude": "-46.7740"},
    {"nome": "Carrao", "latitude": "-23.5582", "longitude": "-46.5374"},
    {"nome": "CasaVerde", "latitude": "-23.5046", "longitude": "-46.6473"},
    {"nome": "CidadeAdemar", "latitude": "-23.6748", "longitude": "-46.6729"},
    {"nome": "CidadeDutra", "latitude": "-23.7005", "longitude": "-46.6934"},
    {"nome": "CidadeLider", "latitude": "-23.5551", "longitude": "-46.4802"},
    {"nome": "CidadeTiradentes", "latitude": "-23.5855", "longitude": "-46.4118"},
    {"nome": "Consolaçao", "latitude": "-23.5515", "longitude": "-46.6619"},
    {"nome": "Cursino", "latitude": "-23.6169", "longitude": "-46.6245"},
    {"nome": "ErmelinoMatarazzo", "latitude": "-23.5048", "longitude": "-46.4766"},
    {"nome": "FreguesiaDoO", "latitude": "-23.4976", "longitude": "-46.6915"},
    {"nome": "Grajau", "latitude": "-23.7505", "longitude": "-46.7156"},
    {"nome": "Guaianases", "latitude": "-23.5311", "longitude": "-46.3981"},
    {"nome": "Iguatemi", "latitude": "-23.6013", "longitude": "-46.4126"},
    {"nome": "Ipiranga", "latitude": "-23.5958", "longitude": "-46.6099"},
    {"nome": "ItaimBibi", "latitude": "-23.5977", "longitude": "-46.6860"},
    {"nome": "ItaimPaulista", "latitude": "-23.4970", "longitude": "-46.4104"},
    {"nome": "Itaquera", "latitude": "-23.5273", "longitude": "-46.4692"},
    {"nome": "Jabaquara", "latitude": "-23.6604", "longitude": "-46.6430"},
    {"nome": "Jaçana", "latitude": "-23.4565", "longitude": "-46.5767"},
    {"nome": "JardimAngela", "latitude": "-23.6912", "longitude": "-46.7826"},
    {"nome": "JardimHelena", "latitude": "-23.5066", "longitude": "-46.4172"},
    {"nome": "JardimPaulista", "latitude": "-23.5645", "longitude": "-46.6601"},
    {"nome": "JardimSaoLuis", "latitude": "-23.6684", "longitude": "-46.7547"},
    {"nome": "JoseBonifacio", "latitude": "-23.5457", "longitude": "-46.4166"},
    {"nome": "Lajeado", "latitude": "-23.5015", "longitude": "-46.3845"},
    {"nome": "Liberdade", "latitude": "-23.5614", "longitude": "-46.6341"},
    {"nome": "Limao", "latitude": "-23.5039", "longitude": "-46.6633"},
    {"nome": "MBoiMirim", "latitude": "-23.6629", "longitude": "-46.7559"},
    {"nome": "Moema", "latitude": "-23.6103", "longitude": "-46.6623"},
    {"nome": "Mooca", "latitude": "-23.5581", "longitude": "-46.5892"},
    {"nome": "Morumbi", "latitude": "-23.6012", "longitude": "-46.7205"},
    {"nome": "Parelheiros", "latitude": "-23.8065", "longitude": "-46.7335"},
    {"nome": "Pari", "latitude": "-23.5242", "longitude": "-46.6133"},
    {"nome": "ParqueDoCarmo", "latitude": "-23.5639", "longitude": "-46.4629"},
    {"nome": "Pedreira", "latitude": "-23.6789", "longitude": "-46.7154"},
    {"nome": "Penha", "latitude": "-23.5335", "longitude": "-46.5421"},
    {"nome": "Perdizes", "latitude": "-23.5389", "longitude": "-46.6786"},
    {"nome": "Perus", "latitude": "-23.3976", "longitude": "-46.7539"},
    {"nome": "Pinheiros", "latitude": "-23.5653", "longitude": "-46.7015"},
    {"nome": "Pirituba", "latitude": "-23.4877", "longitude": "-46.7412"},
    {"nome": "PonteRasa", "latitude": "-23.5083", "longitude": "-46.5139"},
    {"nome": "RaposoTavares", "latitude": "-23.6051", "longitude": "-46.7493"},
    {"nome": "Republica", "latitude": "-23.5434", "longitude": "-46.6395"},
    {"nome": "RioPequeno", "latitude": "-23.5725", "longitude": "-46.7403"},
    {"nome": "Sacoma", "latitude": "-23.6275", "longitude": "-46.5898"},
    {"nome": "SantaCecilia", "latitude": "-23.5378", "longitude": "-46.6491"},
    {"nome": "Santana", "latitude": "-23.5055", "longitude": "-46.6277"},
    {"nome": "SantoAmaro", "latitude": "-23.6542", "longitude": "-46.7102"},
    {"nome": "SãoDomingos", "latitude": "-23.4809", "longitude": "-46.7513"},
    {"nome": "SãoLucas", "latitude": "-23.6025", "longitude": "-46.5385"},
    {"nome": "SãoMateus", "latitude": "-23.6001", "longitude": "-46.4697"},
    {"nome": "SãoMiguel", "latitude": "-23.4996", "longitude": "-46.4399"},
    {"nome": "SãoRafael", "latitude": "-23.6170", "longitude": "-46.4121"},
    {"nome": "Sapopemba", "latitude": "-23.5893", "longitude": "-46.5149"},
    {"nome": "Saude", "latitude": "-23.6204", "longitude": "-46.6373"},
    {"nome": "Se", "latitude": "-23.5505", "longitude": "-46.6333"},
    {"nome": "Socorro", "latitude": "-23.6884", "longitude": "-46.7079"},
    {"nome": "Tatuape", "latitude": "-23.5402", "longitude": "-46.5759"},
    {"nome": "Tremembe", "latitude": "-23.4511", "longitude": "-46.6206"},
    {"nome": "Tucuruvi", "latitude": "-23.4781", "longitude": "-46.6207"},
    {"nome": "VilaAndrade", "latitude": "-23.6253", "longitude": "-46.7359"},
    {"nome": "VilaCuruça", "latitude": "-23.4944", "longitude": "-46.3984"},
    {"nome": "VilaFormosa", "latitude": "-23.5734", "longitude": "-46.5565"},
    {"nome": "VilaGuilherme", "latitude": "-23.5154", "longitude": "-46.6057"},
    {"nome": "VilaJacui", "latitude": "-23.4984", "longitude": "-46.4274"},
    {"nome": "VilaLeopoldina", "latitude": "-23.5278", "longitude": "-46.7267"},
    {"nome": "VilaMaria", "latitude": "-23.5044", "longitude": "-46.5837"},
    {"nome": "VilaMariana", "latitude": "-23.5880", "longitude": "-46.6327"},
    {"nome": "VilaMatilde", "latitude": "-23.5369", "longitude": "-46.5130"},
    {"nome": "VilaMedeiros", "latitude": "-23.4911", "longitude": "-46.5816"},
    {"nome": "VilaPrudente", "latitude": "-23.5862", "longitude": "-46.5822"},
    {"nome": "VilaSonia", "latitude": "-23.6017", "longitude": "-46.7229"}
]

# Lista de parâmetros meteorológicos
parametros = 'wind_speed_10m:ms,wind_dir_10m:d,wind_gusts_10m_1h:ms,t_2m:C,t_max_2m_24h:C,t_min_2m_24h:C,weather_symbol_1h:idx,uv:idx,msl_pressure:hPa,precip_1h:mm,precip_24h:mm'

# Função para extrair dados
def extract_data(execution_date, latitude, longitude, location_name):
    start_date = pendulum.parse(execution_date)
    end_date = start_date.add(days=3)
    start_date_str = start_date.to_iso8601_string()
    end_date_str = end_date.to_iso8601_string()

    # Construir a URL com os parâmetros meteorológicos
    url = f'https://api.meteomatics.com/{start_date_str}--{end_date_str}:PT1H/{parametros}/{latitude},{longitude}/csv'
    
    print(f"Requesting data from URL: {url}")  # Debug: Imprimir URL para verificar
    response = requests.get(url, auth=HTTPBasicAuth(username, password))
    
    if response.status_code != 200:
        print(f"Failed to retrieve data: {response.text}")
        response.raise_for_status()

    data = pd.read_csv(StringIO(response.text))
    
    base_path = f'C:/Users/WORK/Desktop/Challenge_Previsao/{location_name}'  # Atualizado para o caminho no container
    if not exists(base_path):
        os.makedirs(base_path)
    
    file_path = join(base_path, f'data_{location_name}_{start_date.strftime("%Y-%m-%d")}_to_{end_date.strftime("%Y-%m-%d")}.csv')
    data.to_csv(file_path, index=False)
    print(f"Data saved to {file_path}")

# Definindo o DAG
with DAG(
        "meteo_bairro96_sp",
        start_date=pendulum.datetime(2024, 9, 28, tz='UTC'),
        schedule_interval='0 0 * * *',
) as dag:
    # Tarefa de início
    start_task = DummyOperator(task_id='start')

    # Tarefas de extração de dados
    for location in locations:
        task = PythonOperator(
            task_id=f'extract_data_{location["nome"]}',
            python_callable=extract_data,
            op_kwargs={
                'execution_date': '{{ ds }}',
                'latitude': location['latitude'],
                'longitude': location['longitude'],
                'location_name': location['nome']
            }
        )
        start_task >> task
