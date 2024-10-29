import geopandas as gpd
from shapely.geometry import LineString, MultiPolygon, MultiLineString
from shapely.ops import split, linemerge
import pandas as pd
import logging
import json
import time
import os



class Splitter:

    def __init__(self, grid_file, input_file,  output_path):
        self.grid_file = grid_file
        self.input_file = input_file
        self.output_path = output_path
        self.grid_gdf = None
        self.input_gdf = None
        self.start_time = time.time()
        #Logger
        self.logger = logging.getLogger(self.__class__.__name__)
        self.logger.setLevel(logging.DEBUG)        
        # Configuração básica de saída para o console
        file_handler = logging.FileHandler('splitter.log')
        formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        file_handler.setFormatter(formatter)
        
        # Adiciona o handler ao logger
        if not self.logger.hasHandlers():  # Evita duplicação de handlers
            self.logger.addHandler(file_handler)

        self.logger.debug(f"Splitter initialized with grid_path: {self.grid_file} AND input_path: {self.input_file}")

    def load_data(self):
        # Carregar o arquivo grid e selecionar o polígono específico
        self.grid_gdf = gpd.read_parquet(self.grid_file)
        self.logger.info(f'grid carregado com sucesso a partir de {self.grid_file}')

        # Carregar o arquivo de entrada
        self.input_gdf = gpd.read_parquet(self.input_file)
        self.logger.info(f'input carregado com sucesso a partir de {self.input_file}')

    def intersection(self, n_grid):
        
        self.n_grid = n_grid

        # Verificar se grid_gdf e input_gdf estão carregados
        if self.grid_gdf is None or self.input_gdf is None:
            self.logger.error("grid_gdf ou input_gdf não estão carregados.")
            raise ValueError("grid_gdf e input_gdf devem estar carregados antes de chamar intersection.")
        
        if self.grid_gdf.empty or self.input_gdf.empty:
            self.logger.error("grid_gdf ou input_gdf estão vazios.")
            raise ValueError("grid_gdf e input_gdf não podem estar vazios.")
        
        self.unidade_split = self.grid_gdf[self.grid_gdf["grid_id"] == self.n_grid].geometry.values[0] 
        self.gdf_input_intersection = self.input_gdf[self.input_gdf.intersects(self.unidade_split)]
        self.logger.info(f'Seleção dos poligonos para split realizada com sucesso, total de {len(self.gdf_input_intersection)} polígonos')
        
    
    
    def prepare_split_line(self):       

        # Criar linhas a partir dos limites dessas geometrias e mesclá-las
        lines = [linemerge(geom.boundary) for geom in self.gdf_input_intersection.geometry]
        lines_gdf=gpd.GeoDataFrame(geometry=lines)
        #Combinação de pontos em uma única linha
        combined_points = []

        for line in lines_gdf.geometry:
            # Verifica se a geometria é LineString ou MultiLineString
            if isinstance(line, LineString):
                # Se for LineString, adiciona as coordenadas diretamente
                combined_points.extend(list(line.coords))
            elif isinstance(line, MultiLineString):
                # Se for MultiLineString, extrai as coordenadas de cada LineString componente
                for linestring in line.geoms:
                    combined_points.extend(list(linestring.coords))
            else:
                # Lança um erro para tipos de geometria não suportados
                raise TypeError(f"Tipo de geometria não suportado: {type(line)}")


        #Cria uma única geometria de Linestring forçada
        self.forced_line = LineString(combined_points)
        self.logger.info(f'Forced line criada com sucesso !')

        return None

    def perform_split(self):
        
        # Inicia o cronômetro para a operação
        operation_start = time.time()
        
        # Dividir o polígono usando a linha forçada
        broken_glass = split(self.unidade_split, self.forced_line)
        self.broken_glass_polygon = MultiPolygon(broken_glass)
        # Criar um GeoDataFrame com os fragmentos
        geometries = list(self.broken_glass_polygon.geoms)
        self.gdf_broken_glass = gpd.GeoDataFrame(data={"id": range(1, len(geometries) + 1)}, 
                                                 geometry=geometries, crs="EPSG:4674")
        elapsed_time = time.time() - operation_start
        self.logger.info(f"Glass shattering complete, levou {elapsed_time:.2f} segundos para o clip do grid {self.n_grid}!")

    def calculate_overlapping(self):
        # Inicia o cronômetro para a operação
        operation_start = time.time()
        
        # Calcular ponto representativo e verificar sobreposição
        self.gdf_broken_glass["representative_point"] = self.gdf_broken_glass.geometry.apply(lambda x: x.representative_point())
        
        # Inicializa listas para armazenar as listas de `id_layer` e `id_feature`
        id_layers_list = []
        id_features_list = []
        
        for index, shard in self.gdf_broken_glass.iterrows():
            glass_shard_point = shard["representative_point"]
            overlapping_polygons = self.input_gdf[self.input_gdf.contains(glass_shard_point)]
            
            # Define id_layers e id_features para cada caso
            if not overlapping_polygons.empty:
                id_layers = ['void'] + overlapping_polygons["id_layer"].tolist()
                id_features = [self.n_grid] + overlapping_polygons["id"].tolist()
            else:
                id_layers = ['void']
                id_features = [self.n_grid]
            
            # Armazena as listas para cada fragmento
            id_layers_list.append(id_layers)
            id_features_list.append(id_features)
        
        # Remover a coluna de ponto representativo
        self.gdf_broken_glass.drop(columns="representative_point", inplace=True)
        
        # Adiciona as listas como novas colunas no gdf_broken_glass
        self.gdf_broken_glass["id_layer"] = id_layers_list
        self.gdf_broken_glass["id_feature"] = id_features_list
        
        # Log com o tempo total de operação
        elapsed_time = time.time() - operation_start
        self.logger.info(f"Cálculo de sobreposição realizado. Mapeados {len(id_layers_list)} cacos de vidro.")
        self.logger.info(f"A operação levou {elapsed_time:.2f} segundos.")
        
        return None


    def save_results(self):
        # Salvar resultados em GeoJSON e Parquet
        self.gdf_broken_glass.to_file(os.path.join(self.output_path, f'split_{self.n_grid}.geojson'), driver="GeoJSON")
        self.gdf_broken_glass.to_parquet(os.path.join(self.output_path, f'split_{self.n_grid}.parquet'))
        self.logger.info(f"Resultados salvos com sucesso da iteração do grid {self.n_grid}.")
        return None


    def run(self, n_grid):
        self.load_data()
        self.intersection(n_grid)
        self.prepare_split_line()
        self.perform_split()
        self.calculate_overlapping()
        self.save_results()
        return None



## Uso da classe Splitter com logging
#if __name__ == "__main__":
#
#
#        #Ler arquivo config.json
#    with open('config.json', 'r') as file:
#        config=json.load(file)
#
#    # Acessando as variáveis carregadas
#    grid_file = config["grid_file"]
#    input_file = config["input_file"]
#    output_path = config["output_path"]
#
#
#    start_time = time.time()
#
#    n_grid = 18
#    splitter = Splitter(grid_file, input_file, output_path)
#    splitter.load_data()
#    splitter.intersection(n_grid)
#    splitter.prepare_split_line()
#    splitter.perform_split()
#    splitter.calculate_overlapping()
#    splitter.save_results()


 
