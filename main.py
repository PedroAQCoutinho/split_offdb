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





# Executa o loop de grid spacing e mede o tempo
if __name__ == "__main__":
    
    start_time = time.time()
    logger.info("Iniciando processamento geral")

    # Carrega a configuração
    with open("config.json", "r") as f:
        config = json.load(f)

    # Skip prepare ?
    skip_prepare_inputs = config["skip_prepare_inputs"]

    dataprocessor = DataProcessor() #usar grid spacing default
    engine=dataprocessor.engine

    if not skip_prepare_inputs:
        # Executa o DataProcessor com o grid_spacing atual      
        dataprocessor.run()
    else:
        print('Skipping prepare_inputs.py')

    #Carrega inputs !!
    #Lista de grids para iteração baseado no grid file gerado
    grids = gpd.read_parquet(config["grid_file"])["grid_id"].tolist()      
    #Carregar camada para split
    #data = load_input(config["input_file"])
    #Carregar o grid
    grid_gdf = gpd.read_parquet(config["grid_file"])


    #Funcao para formatar array
    create_query=f"CREATE SCHEMA IF NOTE EXISTS split;CREATE TABLE IF NOT EXISTS split.{config['arquivos_final']} (id integer, id_layer text[], id_feature integer[], geometry geometry(polygon, 4674))"
    with engine.connect() as conn:
        with conn.begin():
            r=conn.execute(text(create_query))

    #Roda o código aqui !!!!!!!!!!
    splitter = Splitter(config_path='config.json')
    splitter.run_parallel(grids=grids, grid_gdf=grid_gdf)

    # Executa o multiprocessing com a lista de grids
    logger.info("Iniciando multiprocessing para grids")
    #Paralelismo baseado em multiprocessing


    # Calcula o tempo decorrido para o grid_spacing atual
    elapsed_time = time.time() - start_time
    logger.info(f"Tempo de processamento: {elapsed_time:.2f} segundos")


    # Tempo total de processamento
    elapsed_total = time.time() - start_time
    logger.info(f"Tempo total de processamento: {elapsed_total:.2f} segundos")




