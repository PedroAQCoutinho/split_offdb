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

# Configuração do logger para main.log
logging.basicConfig(
    filename='logs/main.log',
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Função para instanciar e executar o Splitter para um grid específico
def run_splitter(grid_id, config):
    splitter = Splitter(config)
    splitter.run(grid_id)

def merge_parquet_files(folder_path, output_file="merged_output"):
    start_merge = time.time()
    # Lista todos os arquivos Parquet na pasta especificada
    parquet_files = [f for f in os.listdir(folder_path) if f.endswith(".parquet")]

    # Lê e armazena todos os GeoDataFrames em uma lista
    gdfs = [gpd.read_parquet(os.path.join(folder_path, file)) for file in parquet_files]

    # Concatena todos os GeoDataFrames em um só, como um empilhamento direto (rbind)
    merged_gdf = gpd.GeoDataFrame(pd.concat(gdfs, ignore_index=True), crs=gdfs[0].crs)
    
    # Salva o GeoDataFrame concatenado em um novo arquivo GeoJSON e Parquet
    output_folder = './finais'
    os.makedirs(output_folder, exist_ok=True)
    
    merged_gdf.to_file(os.path.join(output_folder, f'{output_file}.geojson'), driver="GeoJSON")
    merged_gdf.to_parquet(os.path.join(output_folder, f'{output_file}.parquet'))
    logger.info(f"Arquivo concatenado salvo como {output_file}")

    # Usa subprocess para excluir arquivos na pasta .outputs
    subprocess.run("rm ./outputs/*", shell=True, check=True)

    elapsed_merge = time.time() - start_merge
    logger.info(f"Tempo para merge dos arquivos Parquet: {elapsed_merge:.2f} segundos")

# Executa o loop de grid spacing e mede o tempo
if __name__ == "__main__":

    start_geral = time.time()
    logger.info("Iniciando processamento geral")

    # Carrega a configuração
    with open("config.json", "r") as f:
        config = json.load(f)

    # Lista para armazenar resultados de tempo para cada grid_spacing
    results = []
    grid_spacing = config["grid_spacing"]
    skip_prepare_inputs = grid_spacing = config["skip_prepare_inputs"]

    # Tempo de início para o grid_spacing atual
    start_time = time.time()

    if not skip_prepare_inputs:
        # Executa o DataProcessor com o grid_spacing atual
        dataprocessor = DataProcessor(grid_spacing=grid_spacing)
        dataprocessor.run()
    else:
        print('Skipping prepare_inputs.py')

    # Usar functools.partial para passar `config` como parâmetro fixo para `run_splitter`
    run_splitter_partial = partial(run_splitter, config="config.json")

    # Carrega os IDs dos grids
    gdf = gpd.read_file("inputs/grid.gpkg")
    grids = gdf["grid_id"].tolist()

    # Executa o multiprocessing com a lista de grids
    logger.info("Iniciando multiprocessing para grids")
    with Pool(processes=config['num_processes']) as pool:
        pool.map(run_splitter_partial, grids)

    # Calcula o tempo decorrido para o grid_spacing atual
    elapsed_time = time.time() - start_time
    logger.info(f"Tempo de processamento para grid_spacing={grid_spacing}: {elapsed_time:.2f} segundos")

    # Adiciona os resultados à lista
    results.append({"grid_spacing": grid_spacing, "elapsed_time": elapsed_time})

    # Limpeza e concatenação dos arquivos após cada iteração
    merge_parquet_files(folder_path=config['output_path'], output_file=f"split_gridspacing_{grid_spacing}")

    # Tempo total de processamento
    elapsed_total = time.time() - start_geral
    logger.info(f"Tempo total de processamento: {elapsed_total:.2f} segundos")

    # Converte os resultados em um DataFrame
    df_results = pd.DataFrame(results)
    # Salva o DataFrame com os resultados em CSV para referência futura
    df_results.to_csv("tempo_grid_spacing.csv", index=False)


