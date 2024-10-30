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

# Função para instanciar e executar o Splitter para um grid específico
def run_splitter(grid_id, config):
    splitter = Splitter(config)
    splitter.run(grid_id)

def merge_parquet_files(folder_path, output_file="merged_output"):
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
    print(f"Arquivo concatenado salvo como {output_file}")

    # Usa subprocess para excluir arquivos na pasta .outputs
    subprocess.run("rm ./outputs/*", shell=True, check=True)






# Executa o loop de grid spacing e mede o tempo
if __name__ == "__main__":



    start_geral = time.time()
    # Carrega a configuração
    with open("config.json", "r") as f:
        config = json.load(f)

    # Lista para armazenar resultados de tempo para cada grid_spacing
    results = []
    grid_spacing_list = [ 0.5]
    # Loop sobre grid_spacing de 1 até 0.1 com passo de -0.01
    for grid_spacing in grid_spacing_list:
        
        # Tempo de início para o grid_spacing atual
        start_time = time.time()

        # Executa o DataProcessor com o grid_spacing atual
        dataprocessor = DataProcessor(grid_spacing=grid_spacing)
        dataprocessor.run()

        # Usar functools.partial para passar `config` como parâmetro fixo para `run_splitter`
        run_splitter_partial = partial(run_splitter, config="config.json")

        # Carrega os IDs dos grids
        with open("inputs/grid.geojson", "r") as f:
            grid = json.load(f)
        grids = [feature["properties"]["grid_id"] for feature in grid["features"]]

        # Executa o multiprocessing com a lista de grids
        with Pool(processes=config['num_processes']) as pool:
            pool.map(run_splitter_partial, grids)

        # Calcula o tempo decorrido
        elapsed_time = time.time() - start_time
        print(f"Demorou {elapsed_time:.2f} segundos para grid_spacing={grid_spacing}")

        # Adiciona os resultados à lista
        results.append({"grid_spacing": grid_spacing, "elapsed_time": elapsed_time})

        # Limpeza e concatenação dos arquivos após cada iteração
        merge_parquet_files(folder_path=config['output_path'], output_file=f"split_gridspacing_{grid_spacing}")

        print(f"Elapsed time {time.time() - start_geral} segundos")

    # Converte os resultados em um DataFrame
    df_results = pd.DataFrame(results)
    # Salva o DataFrame com os resultados em CSV para referência futura
    df_results.to_csv("tempo_grid_spacing.csv", index=False)
    # Exibe os resultados em um gráfico de linha
    plt.figure(figsize=(10, 6))
    plt.plot(df_results["grid_spacing"], df_results["elapsed_time"], marker="o")
    plt.xlabel("Grid Spacing")
    plt.ylabel("Elapsed Time (seconds)")
    plt.title("Tempo de Processamento em Função do Grid Spacing")
    plt.gca().invert_xaxis()  # Inverte eixo para exibir grid_spacing decrescente
    plt.show()


