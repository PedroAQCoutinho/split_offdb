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
    grids = gpd.read_parquet(config["grid_file"])["id"].tolist()      
    #Carregar camada para split
    #data = load_input(config["input_file"])
    #Carregar o grid
    grid_gdf = gpd.read_parquet(config["grid_file"])

    #Roda o código aqui !!!!!!!!!!
    logger.info("Iniciando multiprocessing para grids")
    splitter = Splitter(config_path='config.json')
    splitter.create_table_postgresql(engine=engine)
    splitter.run_parallel(grids=grids, grid_gdf=grid_gdf)
    splitter.create_indices(engine=engine)



    # #Cria indices no banco
    # queries_index = [
    #     f"create index if not exists idx_{config['arquivos_final']}_gid on split.{config['arquivos_final']} (gid);",
    #     f"create index if not exists idx_{config['arquivos_final']}_id_layer on split.{config['arquivos_final']} (id_layer);",
    #     f"create index if not exists idx_{config['arquivos_final']}_geom on split.{config['arquivos_final']} using gist (geometry);",
    #     f"create index if not exists idx_{config['arquivos_final']}_id_feature on split.{config['arquivos_final']} (id_feature);"]

    # with engine.connect() as conn:
    #     for q in queries_index:
    #         print(q)
    #         with conn.begin():
    #             r=conn.execute(text(q))


    

    # Calcula o tempo decorrido para o grid_spacing atual
    elapsed_time = time.time() - start_time
    logger.info(f"Tempo de processamento: {elapsed_time:.2f} segundos")


    # Tempo total de processamento
    elapsed_total = time.time() - start_time
    logger.info(f"Tempo total de processamento: {elapsed_total:.2f} segundos")




