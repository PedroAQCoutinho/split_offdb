from multiprocessing import Pool
from split import Splitter
import time
import json
from functools import partial

# Defina a função para instanciar e executar o Splitter
def run_splitter(grid_file, input_file, output_path, n):
    splitter = Splitter(grid_file, input_file, output_path)
    splitter.run(n)

# Executa o loop em paralelo
if __name__ == "__main__":

    # Ler arquivo config.json
    with open('config.json', 'r') as file:
        config = json.load(file)

    # Acessando as variáveis carregadas
    grid_file = config["grid_file"]
    input_file = config["input_file"]
    output_path = config["output_path"]
    num_processes = config["num_processes"]

    # Define o tempo de início
    start_time = time.time()

    # Usa partial para fixar os primeiros três argumentos
    run_splitter_partial = partial(run_splitter, grid_file, input_file, output_path)
    
    with Pool(processes=num_processes) as pool:
        pool.map(run_splitter_partial, range(1, 31))

    # Calcula o tempo decorrido
    elapsed_time = time.time() - start_time
    print(f'Demorou {elapsed_time:.2f} segundos para rodar tudo')
