import geopandas as gpd
from shapely.geometry import MultiLineString, LineString, Polygon, MultiPolygon, LinearRing
from shapely.ops import split, linemerge, polygonize
import pandas as pd
import logging
import json
import time
import os
import shapely as shp
from multiprocessing import Pool
from functools import partial
import numpy as np
from rtree import index
import psutil


def load_input(input_file):
    # Carregar o arquivo de entrada
    input_gdf = gpd.read_parquet(input_file)    
    print('Input carregado com sucesso !')
    return input_gdf

class Splitter:

    def __init__(self, data=None, config_path="config.json"):

        if data is None:
            raise ValueError("Data cannot be None")
        
        # Carregar configuração do arquivo JSON
        with open(config_path, "r") as f:
            config = json.load(f)

        # Acessando as variáveis carregadas
        self.grid_file = config["grid_file"]
        self.input_file = config["input_file"]
        self.output_path = config["output_path"]
        self.num_processes = config["num_processes"]
        self.memory = psutil.virtual_memory()

        # Cria vazios
        self.grid_gdf = None
        self.input_gdf = None
        self.start_time = time.time()
        
        #Logger
        self.logger = logging.getLogger(self.__class__.__name__)
        self.logger.setLevel(logging.DEBUG)        
        #self.logger.propagate = False  # Evita que os logs do splitter apareçam em main.log
        
        # Configuração básica de saída para o console
        file_handler = logging.FileHandler('logs/splitter.log', mode='w', encoding='utf-8')

        formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        file_handler.setFormatter(formatter)
        
        # Adiciona o handler ao logger
        if not self.logger.hasHandlers():  # Evita duplicação de handlers
            self.logger.addHandler(file_handler)

        self.logger.debug(f"Splitter initialized with grid_path: {self.grid_file} AND input_path: {self.input_file}")


        # Cria um índice espacial para `input_gdf`
        self.input_sindex = data.sindex
        logging.info('Indices criados com sucesso !')




    def _intersection(self, n_grid, data, grid_gdf):
        start_time=time.time()
        self.n_grid = n_grid

        try:
            # Verificar se grid_gdf e input_gdf estão carregados
            if grid_gdf is None or data is None:
                self.logger.error("grid_gdf ou input_gdf nao estao carregados.")
                raise ValueError("grid_gdf e input_gdf devem estar carregados antes de chamar intersection.")
            
            if grid_gdf.empty or data.empty:
                self.logger.error("grid_gdf ou input_gdf estao vazios.")
                raise ValueError("grid_gdf e input_gdf não podem estar vazios.")

            # Seleciona a unidade específica no grid e aplica índice espacial para otimizar a interseção
            self.unidade_split = grid_gdf[grid_gdf["grid_id"] == self.n_grid].geometry.values[0]
    
            


            # Filtra apenas as geometrias que têm bbox sobrepondo `unidade_split`
            possible_matches_index = list(self.input_sindex.intersection(self.unidade_split.bounds))
            possible_matches = data.iloc[possible_matches_index]

            

            # Calcula as interseções reais e armazena apenas as interseções não vazias
            intersections=[]
            intersecting_ids=[]
            intersecting_id_layers=[]
            # Itera sobre as geometrias
            for index, row in possible_matches.iterrows():
                geom=row.geom
                if isinstance(geom, Polygon):
                    geom=geom
                elif isinstance(geom, MultiPolygon):
                    geom=geom.geoms[0]

                if geom.is_valid:
                    intersection = self.unidade_split.intersection(geom)
                    if isinstance(intersection, Polygon):
                        intersection=intersection
                    elif isinstance(intersection, MultiPolygon):
                        intersection=intersection.geoms[0]
                else:
                    #pula para o proximo loop, de modo a eliminar geometria inválida
                    continue


                # Checa se a interseção é válida e não está vazia
                if not intersection.is_empty and intersection.is_valid:
                    intersections.append(intersection)
                    intersecting_ids.append(row['id'])  # Armazena o 'id' da geometria original
                    intersecting_id_layers.append(row['id_layer'])
                else:
                    #Continue para o próximo loop, descartando geometria inválida
                    continue

            # Cria um novo GeoDataFrame com as interseções reais
            self.gdf_input_intersection = gpd.GeoDataFrame(
                    data={'id': intersecting_ids, 'id_layer': intersecting_id_layers, 'geom': intersections},  # Inclui o ID original da geometria que intersectou
                    geometry='geom',
                    crs='EPSG:4674')
        except Exception as e:
            logging.error(f'Função _intersect na iteração {self.n_grid} deu o problema {e}')

        elapsed_time=time.time()-start_time
        return f'{elapsed_time:.2f}'




    def intersection(self, n_grid, data, grid_gdf):
            
        self.n_grid = n_grid

        # Verificar se grid_gdf e input_gdf estão carregados
        if grid_gdf is None or data is None:
            self.logger.error("grid_gdf ou input_gdf nao estao carregados.")
            raise ValueError("grid_gdf e input_gdf devem estar carregados antes de chamar intersection.")
        
        if grid_gdf.empty or data.empty:
            self.logger.error("grid_gdf ou input_gdf estao vazios.")
            raise ValueError("grid_gdf e input_gdf não podem estar vazios.")

        # Seleciona a unidade específica no grid e aplica índice espacial para otimizar a interseção
        self.unidade_split = grid_gdf[grid_gdf["grid_id"] == self.n_grid].geometry.values[0]
        
        # Cria um índice espacial para `input_gdf`
        #self.input_sindex = data.sindex

        # Filtra apenas as geometrias que têm bbox sobrepondo `unidade_split`
        possible_matches_index = list(self.input_sindex.intersection(self.unidade_split.bounds))
        possible_matches = data.iloc[possible_matches_index]

        # Filtra apenas as interseções reais
        self.gdf_input_intersection = possible_matches[possible_matches.intersects(self.unidade_split)]

        #self.logger.info(f'Seleção dos polígonos para split realizada com sucesso, total de {len(self.gdf_input_intersection)} polígonos')
        return self.gdf_input_intersection
        
    
    def prepare_split_line(self):
        self.counter=0
        start_time=time.time()
        """Essa funcao é a mais complicada do código
        O que ela se propõe a fazer é simples: Gerar uma MultiLinestring que será inputada no shp.node()
        Essa multilinestring deve ser construída da maneira mais manual possível"""
        try:
            linerings=[]
            #Temos que assumir que todos os poligonos de entrada devem formar lines rings. Caso isso não seja possível a geometria deve ser descartada
            for index, row in self.gdf_input_intersection.iterrows():
                #Seleciona geometria do dado
                geom=row.geom           
                #Se for multipolygon, seleciona o primeiro polygon da feição e pega o boundary. Aqui todas as geometria sao validas
                if isinstance(geom, MultiPolygon):   
                    geom= geom.geoms[0]
                    line=geom.exterior
                elif isinstance(geom, Polygon):
                    geom = geom
                    line=geom.exterior # A funcao exterior é um sacada, ao invés de usar a boundary. Ler documentacao para compreender.

                #Se a linha for válida, appenda na lista de linearRings, se não for válida, joga no lixo pelo bem da humanidade
                if line.is_valid:
                    line=LinearRing(line)
                    linerings.append(line)
                else:
                    self.counter+=1
                    print(f"Feição descartada - ID: {row['id']}, ID Layer: {row['name']}, Geometria: {geom}")
                    #passa pro promixo loop
                    continue   

            
            try:            
                #Appenda o grid, para que seja feita a reconstrucao total do grid
                linerings.append(LinearRing(self.unidade_split.exterior))
                #Cria um MultiLineString a partir de todas as linhas
                multi_line=MultiLineString(linerings)
                #Cria o MultiLineString com nós
                self.multi_line_with_nodes=shp.node(multi_line)        
            except Exception as e:
                logging.error(f'Nâo foi possivel formar o MultiLinestring pelo motivo {e}')

        except Exception as e:
            logging.error(f'Função prepare_split_line na iteração {self.n_grid} deu o problema {e}')

        elapsed_time=time.time()-start_time
        return f'{elapsed_time:.2f}'

    def perform_split(self):
        
        # Inicia o cronômetro para a operaçãorm ou  
        start_time = time.time()
        try:
            # Dividir o polígono usando a linha forçada
            broken_glass_polygon = list(polygonize(self.multi_line_with_nodes))
            
            
            # Filtra apenas os polígonos cujo representative_point intersecta unidade_split
            filtered_polygons = [
                poly for poly in broken_glass_polygon 
                if poly.representative_point().intersects(self.unidade_split)
            ]
            
            del self.unidade_split
            self.gdf_broken_glass = gpd.GeoDataFrame(data={"id": range(1, len(filtered_polygons) + 1)}, 
                                                    geometry=filtered_polygons, crs="EPSG:4674")
            #elapsed_time = time.time() - operation_start
            #self.logger.info(f"Glass shattering complete, levou {elapsed_time:.2f} segundos para o clip do grid {self.n_grid}!")
            del self.multi_line_with_nodes
        except Exception as e:
            logging.error(f'Função perform_split na iteração {self.n_grid} deu o problema {e}')

        elapsed_time=time.time()-start_time
        return f'{elapsed_time:.2f}'





    def calculate_overlapping(self):
        # Inicia o cronômetro para a operação
        start_time = time.time()
        try:
            # Calcular ponto representativo para cada geometria
            self.gdf_broken_glass["representative_point"] = self.gdf_broken_glass.geometry.apply(lambda x: x.representative_point())

            # Inicializa listas para armazenar os `id_layer` e `id_feature`
            id_layers_list = []
            id_features_list = []

            # Cria um índice espacial para os polígonos em gdf_input_intersection
            spatial_index = index.Index()
            for idx, poly in self.gdf_input_intersection.iterrows(): 
                geom=poly.geom
                spatial_index.insert(idx, geom.bounds)

            # Itera sobre cada fragmento de vidro
            for idx, shard in self.gdf_broken_glass.iterrows():
                glass_shard_point = shard["representative_point"]

                # Encontra os índices dos polígonos candidatos usando o índice espacial
                possible_matches_index = list(spatial_index.intersection(glass_shard_point.bounds))
                possible_matches = self.gdf_input_intersection.iloc[possible_matches_index]

                # Filtra os polígonos que realmente contêm o ponto representativo
                overlapping_polygons = possible_matches[possible_matches.contains(glass_shard_point)]

                # Define id_layers e id_features para cada caso
                if not overlapping_polygons.empty:
                    id_layers = ['GRID'] + overlapping_polygons["id_layer"].tolist()
                    id_features = [self.n_grid] + overlapping_polygons["id"].tolist()
                else:
                    id_layers = ['GRID']
                    id_features = [self.n_grid]

                # Armazena as listas para cada fragmento
                id_layers_list.append(id_layers)
                id_features_list.append(id_features)

            # Remove a coluna de ponto representativo
            self.gdf_broken_glass.drop(columns="representative_point", inplace=True)

            # Adiciona as listas como novas colunas no gdf_broken_glass
            self.gdf_broken_glass["id_layer"] = id_layers_list
            self.gdf_broken_glass["id_feature"] = id_features_list

        except Exception as e:
            logging.error(f'Função calculate_overlapping na iteração {getattr(self, "n_grid", "N/A")} deu o problema {e}')

        # Limpa o gdf_input_intersection da memória
        del self.gdf_input_intersection

        # Log do tempo total de operação
        elapsed_time = time.time() - start_time
        return f'{elapsed_time:.2f}'



    def save_results(self):
        start_time=time.time()
        memory = psutil.virtual_memory()
        cpu_percent = psutil.cpu_percent(interval=0.1)
        # Salvar resultados em GeoJSON e Parquet
        #self.gdf_broken_glass.to_file(os.path.join(self.output_path, f'split_{self.n_grid}.geojson'), driver="GeoJSON")
        self.gdf_broken_glass.to_parquet(os.path.join(self.output_path, f'split_{self.n_grid}.parquet'))
        self.logger.info(f"Iteração do grid {self.n_grid} armazenada - Uso de memória : {memory.percent}% - CPU : {cpu_percent}%")
        
        del self.gdf_broken_glass
        elapsed_time=time.time()-start_time
        return f'{elapsed_time:.2f}'


    def run(self, n_grid, data, grid_gdf):
        # Função que processa cada grid específico
        start_time=time.time()
        try:
            intersection_time=self._intersection(n_grid, data, grid_gdf)
            prepare_lines_time=self.prepare_split_line()
            perform_split_time=self.perform_split()
            overlapping_time=self.calculate_overlapping()
            save_time=self.save_results()
            elapsed_time=time.time()-start_time
            tempos={'intersection_time':intersection_time,
                    'prepare_lines_time':prepare_lines_time,
                    'perform_split_time':perform_split_time,
                    'overlapping_time':overlapping_time,
                    'save_time':save_time}
            # Encontrar o maior tempo e a chave correspondente
            max_time_func, max_time_value = max(tempos.items(), key=lambda item: item[1])
            logging.info(f'Iteração completa para o {n_grid} levou {elapsed_time:.2f} e a operação que levou mais tempo foi a funcao {max_time_func} com {max_time_value} e descartou {self.counter} feicoes')
        #Se der erro prossegue 
        except Exception as e:
            # Registra o n_grid no arquivo de erro
            with open("logs/error_grids.txt", "a") as error_file:
                error_file.write(f"{n_grid}\n")
            self.logger.error(f"Iteração do grid {self.n_grid} ERRO {e}")
            
        


    def run_parallel(self, data, grids,grid_gdf):
        run_splitter_partial = partial(self.run, data=data,grid_gdf=grid_gdf)
        # Função para execução paralela
        with Pool(processes=self.num_processes) as pool:
            pool.map(run_splitter_partial, grids)



# # Uso da classe Splitter com logging

# if __name__ == "__main__":


#     start_time = time.time()

#     with open("config.json", "r") as f:
#         config = json.load(f)  

#     data = load_input(config["input_file"])
   
#     splitter = Splitter()
#     splitter.load_data()
#     splitter._intersection(13, data) # Foi criada a _intersection pois a antiga fazia apenas o touches, o que sobrecarregava a memoria
#     splitter.intersection(13, data)
#     splitter.prepare_split_line()
#     splitter.perform_split()
#     splitter.calculate_overlapping()
#     splitter.save_results()


 
