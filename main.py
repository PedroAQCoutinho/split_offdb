import logging
from multiprocessing import Pool
from split import Splitter
import time
import json
from prepare_inputs import DataProcessor
from functools import partial
import geopandas as gpd
import pandas as pd
import os
import matplotlib.pyplot as plt
import numpy as np
import subprocess
from rtree import index
from utils import merge_parquet_files
from sqlalchemy import text, create_engine




# Carrega a configuração
with open("config.json", "r") as f:
    config = json.load(f)

logfile = f"logs/main_{config['output']['tabela_saida']}.log"

# Configuração do logger para main.log
logging.basicConfig(
    filename=logfile,
    level=logging.INFO,
    filemode = 'w',
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


#Cria diretórios necessários
os.makedirs('inputs', exist_ok=True)
os.makedirs('outputs', exist_ok=True)
os.makedirs('logs', exist_ok=True)
os.makedirs('finais', exist_ok=True)





# Dispara o processo quando executado a partir da linha de comando (python main.py)
if __name__ == "__main__":
    
    start_time = time.time()
    logger.info(f"Iniciando processamento geral, PID : {os.getpid()}")


    try:
        dataprocessor = DataProcessor() #usar grid spacing default
        engine=dataprocessor.engine
        dataprocessor = DataProcessor()
        dataprocessor.check_schema()
        dataprocessor.create_grid() 
        dataprocessor.create_input()
        # Calcula o tempo decorrido
        elapsed_time = time.time() - start_time
        logging.info(f'Demorou {elapsed_time:.2f} segundos para rodar a preparação dos inputs')
    except Exception as e:
        logging.error(f"Erro na execução de dataprocessor {e}")


    #Carregamento do grid na memória
    ##### Aqui pode ser um ponto de melhoria. Nao carregar na memoria ##### 

    #Lista de grids para iteração baseado no grid file gerado
    rows = dataprocessor.run_sql(
        sql=f"SELECT distinct id FROM {config['grid']['schema']}.{config['output']['tabela_saida']}_grid",
        fetch=True
    )[0]  # pega o primeiro elemento da lista externa
    #lista de grids
    grids = [r[0] for r in rows]  # extrai o campo id
    logging.info(f"GRIDS: {grids}")
    try:
        #Roda o código aqui !!!!!!!!!!
        logger.info("Iniciando multiprocessing para grids")
        #Intancia a classe
        splitter = Splitter(config_path='config.json')
        #Cria a tabela
        splitter.create_table(engine=engine)
        #Cria indices na tabela final no db
        splitter.create_indices(engine=engine)
        #Roda em paralelo diversos grids, que utilizam recursos da instancia principal
        splitter.run_parallel(grids=grids)
        # Tempo total de processamento
        elapsed_total = time.time() - start_time
        logger.info(f"Tempo total de processamento: {elapsed_total:.2f} segundos")
    except Exception as e:
        logging.error(f"Erro {e}")





