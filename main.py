from mpi4py import MPI
import logging
import json
import time
import os
import geopandas as gpd
import pandas as pd
import numpy as np
from split import Splitter, load_input
from prepare_inputs import DataProcessor
import subprocess


# Configuração do logger para main.log
logging.basicConfig(
    filename='logs/main.log',
    level=logging.INFO,
    filemode='w',
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


# Função para mesclar os arquivos Parquet gerados
def merge_parquet_files(folder_path, output_file="merged_output"):
    start_merge = time.time()
    parquet_files = [f for f in os.listdir(folder_path) if f.endswith(".parquet")]
    gdfs = [gpd.read_parquet(os.path.join(folder_path, file)) for file in parquet_files]
    merged_gdf = gpd.GeoDataFrame(pd.concat(gdfs, ignore_index=True), crs=gdfs[0].crs)
    merged_gdf.to_file(os.path.join('./finais', f'{output_file}.gpkg'), driver="GPKG")
    merged_gdf.to_parquet(os.path.join('./finais', f'{output_file}.parquet'))
    logger.info(f"Arquivo concatenado salvo como {output_file}")
    subprocess.run("rm ./outputs/*", shell=True, check=True)
    elapsed_merge = time.time() - start_merge
    logger.info(f"Tempo para merge dos arquivos Parquet: {elapsed_merge:.2f} segundos")


# Função principal para execução em HPC
if __name__ == "__main__":
    # Inicialização do MPI
    comm = MPI.COMM_WORLD
    rank = comm.Get_rank()
    size = comm.Get_size()

    start_time = time.time()
    if rank == 0:
        logger.info("Iniciando processamento geral")

        # Carrega a configuração
        with open("config.json", "r") as f:
            config = json.load(f)

        # Diretórios necessários
        os.makedirs('inputs', exist_ok=True)
        os.makedirs('outputs', exist_ok=True)
        os.makedirs('logs', exist_ok=True)
        os.makedirs('finais', exist_ok=True)

        # Preparação dos dados de entrada, executada apenas pelo processo mestre
        if not config["skip_prepare_inputs"]:
            dataprocessor = DataProcessor()
            dataprocessor.run()
        else:
            print("Skipping prepare_inputs.py")

        # Carregar dados e grids
        data = load_input(config["input_file"])
        grid_gdf = gpd.read_parquet(config["grid_file"])
        grids = grid_gdf["grid_id"].tolist()

        # Divide os grids entre os processos
        grids_split = np.array_split(grids, size)
    else:
        # Inicializa variáveis para processos que não são o mestre
        config = None
        data = None
        grid_gdf = None
        grids_split = None

    # Broadcast da configuração e dos dados para todos os processos
    config = comm.bcast(config, root=0)
    data = comm.bcast(data, root=0)
    grid_gdf = comm.bcast(grid_gdf, root=0)
    grids_local = comm.scatter(grids_split, root=0)

    # Cada processo executa sua parte dos grids
    splitter = Splitter(config_path="config.json")
    for grid_id in grids_local:
        splitter.run(grid_id, data=data, grid_gdf=grid_gdf)

    # Sincronização dos processos
    comm.Barrier()
    
    if rank == 0:
        # Somente o mestre mescla os arquivos Parquet após todos os processos terminarem
        merge_parquet_files(folder_path=config['output_path'], output_file=config["arquivos_final"])
        
        elapsed_total = time.time() - start_time
        logger.info(f"Tempo total de processamento: {elapsed_total:.2f} segundos")
