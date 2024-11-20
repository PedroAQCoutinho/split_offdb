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
import re

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




# Função para instanciar e executar o Splitter para um grid específico
def run_splitter(grid_id, data, config):
    splitter = Splitter(config)
    splitter.run(grid_id, data)

def merge_parquet_files(folder_path, output_file="merged_output"):
    start_merge = time.time()
    # Lista todos os arquivos Parquet na pasta especificada
    parquet_files = [f for f in os.listdir(folder_path) if f.endswith(".parquet")]

    # Lê e armazena todos os GeoDataFrames em uma lista
    gdfs = [gpd.read_parquet(os.path.join(folder_path, file)) for file in parquet_files]

    # Concatena todos os GeoDataFrames em um só, como um empilhamento direto (rbind)
    merged_gdf = gpd.GeoDataFrame(pd.concat(gdfs, ignore_index=True), crs=gdfs[0].crs)
    
    #Export    
    merged_gdf.to_file(os.path.join('./finais', f'{output_file}.gpkg'), driver="gpkg")
    merged_gdf.to_parquet(os.path.join('./finais', f'{output_file}.parquet'))
    logger.info(f"Arquivo concatenado salvo como {output_file}")

    # Usa subprocess para excluir arquivos na pasta .outputs
    subprocess.run("rm ./outputs/*", shell=True, check=True)

    elapsed_merge = time.time() - start_merge
    logger.info(f"Tempo para merge dos arquivos Parquet: {elapsed_merge:.2f} segundos")

# Executa o loop de grid spacing e mede o tempo
if __name__ == "__main__":
    
    start_time = time.time()
    logger.info("Iniciando processamento geral")

    # Carrega a configuração
    with open("config.json", "r") as f:
        config = json.load(f)

    # Skip prepare ?
    skip_prepare_inputs = config["skip_prepare_inputs"]


    if not skip_prepare_inputs:
        # Executa o DataProcessor com o grid_spacing atual
        dataprocessor = DataProcessor() #usar grid spacing default
        dataprocessor.run()
    else:
        print('Skipping prepare_inputs.py')


    #Carrega inputs !!
    #Lista de grids para iteração baseado no grid file gerado
    grids = gpd.read_parquet(config["grid_file"])["grid_id"].tolist()     
    files = os.listdir(config['output_path'])
    numbers = [int(re.search(r'\d+', filename).group()) for filename in files if re.search(r'\d+', filename)]
    grids = [grid for grid in grids if grid not in numbers]
 
    #Carregar camada para split
    data = load_input(config["input_file"])

    #Carregar o grid
    grid_gdf = gpd.read_parquet(config["grid_file"])

    #Roda o código aqui !!!!!!!!!!
    splitter = Splitter(data, config_path='config.json')
    splitter.run_parallel(data=data, grids=grids, grid_gdf=grid_gdf)

    # Executa o multiprocessing com a lista de grids
    logger.info("Iniciando multiprocessing para grids")
    #Paralelismo baseado em multiprocessing


    # Calcula o tempo decorrido para o grid_spacing atual
    elapsed_time = time.time() - start_time
    logger.info(f"Tempo de processamento: {elapsed_time:.2f} segundos")



    # Limpeza e concatenação dos arquivos após cada iteração
    merge_parquet_files(folder_path=config['output_path'], output_file=config["arquivos_final"])

    # Tempo total de processamento
    elapsed_total = time.time() - start_time
    logger.info(f"Tempo total de processamento: {elapsed_total:.2f} segundos")




