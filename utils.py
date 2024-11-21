import time
import geopandas as gpd
import os
import pandas as pd
import logging
from split import Splitter
import subprocess
import numpy as np


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
    logging.info(f"Arquivo concatenado salvo como {output_file}")

    # Usa subprocess para excluir arquivos na pasta .outputs
    subprocess.run("rm ./outputs/*", shell=True, check=True)

    elapsed_merge = time.time() - start_merge
    logging.info(f"Tempo para merge dos arquivos Parquet: {elapsed_merge:.2f} segundos")


def format_array(column):
    """
    Converte valores de uma coluna para o formato de array do PostgreSQL.
    """
    return column.apply(lambda x: '{' + ','.join(map(str, x)) + '}' if isinstance(x, np.ndarray) else None)