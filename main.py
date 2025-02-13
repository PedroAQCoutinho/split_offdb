import logging
from multiprocessing import Pool
from split import Splitter, load_input
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
from uploader import upload_parquet, upload_full_folder



# Configuração do logger para main.log
logging.basicConfig(
    filename='logs/main.log',
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

    # Carrega a configuração
    with open("config.json", "r") as f:
        config = json.load(f)

    # Skip prepare ?
    skip_prepare_inputs = config["skip_prepare_inputs"]

    dataprocessor = DataProcessor() #usar grid spacing default
    engine=dataprocessor.engine

    if not skip_prepare_inputs:
        # Executa o DataProcessor com o grid_spacing atual e gera os inputs conforme as queries do config.json
        dataprocessor.run()
    else:
        print('Skipping prepare_inputs.py')

    #Carregamento do grid na memória
    ##### Aqui pode ser um ponto de melhoria. Nao carregar na memoria ##### 

    #Lista de grids para iteração baseado no grid file gerado

    grids = gpd.read_parquet(config["grid_file"])["id"].tolist()      



    #Carregar o grid vetorial
    grid_gdf = gpd.read_parquet(config["grid_file"])

    

    #Roda o código aqui !!!!!!!!!!
    logger.info("Iniciando multiprocessing para grids")
    splitter = Splitter(config_path='config.json')
    splitter.create_table_postgresql(engine=engine)
    splitter.run_parallel(grids=grids, grid_gdf=grid_gdf)
    splitter.create_indices(engine=engine)

    # Tempo total de processamento
    elapsed_total = time.time() - start_time
    logger.info(f"Tempo total de processamento: {elapsed_total:.2f} segundos")




