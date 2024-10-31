import geopandas as gpd
from shapely.geometry import LineString, MultiPolygon, MultiLineString
from shapely.ops import split, linemerge, polygonize
import pandas as pd
import logging
import json
import time
import os
import shapely as shp


class Splitter:

    def __init__(self, config_path="config.json"):

                # Carregar configuração do arquivo JSON
        with open(config_path, "r") as f:
            config = json.load(f)

        # Acessando as variáveis carregadas
        self.grid_file = config["grid_file"]
        self.input_file = config["input_file"]
        self.output_path = config["output_path"]
        self.num_processes = config["num_processes"]

        # Cria vazios
        self.grid_gdf = None
        self.input_gdf = None
        self.start_time = time.time()
        
        #Logger
        self.logger = logging.getLogger(self.__class__.__name__)
        self.logger.setLevel(logging.DEBUG)        
        
        # Configuração básica de saída para o console
        file_handler = logging.FileHandler('logs/splitter.log', mode = 'w')
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

        # Seleciona a unidade específica no grid e aplica índice espacial para otimizar a interseção
        self.unidade_split = self.grid_gdf[self.grid_gdf["grid_id"] == self.n_grid].geometry.values[0]
        
        # Cria um índice espacial para `input_gdf`
        input_sindex = self.input_gdf.sindex

        # Filtra apenas as geometrias que têm bbox sobrepondo `unidade_split`
        possible_matches_index = list(input_sindex.intersection(self.unidade_split.bounds))
        possible_matches = self.input_gdf.iloc[possible_matches_index]

        # Filtra apenas as interseções reais
        self.gdf_input_intersection = possible_matches[possible_matches.intersects(self.unidade_split)]
        
        self.logger.info(f'Seleção dos polígonos para split realizada com sucesso, total de {len(self.gdf_input_intersection)} polígonos')

        
    
    def prepare_split_line(self):       

        # Criar linhas a partir dos limites dessas geometrias e mesclá-las
        lines = []

        for geom in self.gdf_input_intersection.geometry:
            merged_line = linemerge(geom.boundary)
            
            if isinstance(merged_line, MultiLineString):
                
                # Se for MultiLineString, combinar seus pontos em uma única LineString
                points = []
                for line in merged_line.geoms:
                    points.extend(line.coords)  # Extrai coordenadas de cada LineString
                
                # Cria uma nova LineString com todos os pontos
                merged_line = LineString(points)
                
            lines.append(merged_line)
                
        lines.append(self.unidade_split.boundary)
          

        # Cria o MultiLineString a partir das linhas
        multi_line = MultiLineString(lines) 
        multi_line = multi_line.simplify(tolerance=0.0001, preserve_topology=True)
        
        # Usa shapely.node para adicionar nós (pontos de interseção) ao MultiLineString
        self.multi_line_with_nodes = shp.node(multi_line)



        
        # lines_gdf=gpd.GeoDataFrame(geometry=lines)
        # #Combinação de pontos em uma única linha
        # combined_points = []

        # for line in lines_gdf.geometry:
        #     # Verifica se a geometria é LineString ou MultiLineString
        #     if isinstance(line, LineString):
        #         # Se for LineString, adiciona as coordenadas diretamente
        #         combined_points.extend(list(line.coords))
        #     elif isinstance(line, MultiLineString):
        #         # Se for MultiLineString, extrai as coordenadas de cada LineString componente
        #         for linestring in line.geoms:
        #             combined_points.extend(list(linestring.coords))
        #     else:
        #         # Lança um erro para tipos de geometria não suportados
        #         raise TypeError(f"Tipo de geometria não suportado: {type(line)}")


        #Cria uma única geometria de Linestring forçada
        # self.forced_line = LineString(combined_points)
        self.logger.info(f'Multi Line criada com sucesso !')

        return None

    def perform_split(self):
        
        # Inicia o cronômetro para a operaçãorm ou  
        operation_start = time.time()
        
        # Dividir o polígono usando a linha forçada
        broken_glass_polygon = list(polygonize(self.multi_line_with_nodes))
        
        # Filtra apenas os polígonos cujo representative_point intersecta unidade_split
        filtered_polygons = [
            poly for poly in broken_glass_polygon 
            if poly.representative_point().intersects(self.unidade_split)
        ]

        self.gdf_broken_glass = gpd.GeoDataFrame(data={"id": range(1, len(filtered_polygons) + 1)}, 
                                                 geometry=filtered_polygons, crs="EPSG:4674")
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
        #self.gdf_broken_glass.to_file(os.path.join(self.output_path, f'split_{self.n_grid}.geojson'), driver="GeoJSON")
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



# # Uso da classe Splitter com logging

# if __name__ == "__main__":


#    start_time = time.time()

 
#    splitter = Splitter()
#    splitter.load_data()
#    splitter.intersection(187)
#    splitter.prepare_split_line()
#    splitter.perform_split()
#    splitter.calculate_overlapping()
#    splitter.save_results()


 
