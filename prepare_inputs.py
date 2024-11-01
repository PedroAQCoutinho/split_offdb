import geopandas as gpd
import json
import os
from sqlalchemy import create_engine
from dotenv import load_dotenv
import time
import logging

class DataProcessor:
    def __init__(self, config_path="config.json", grid_spacing=0.5):
        # Configuração do logger
        logging.basicConfig(
            filename='logs/prepare_inputs.log', 
            level=logging.INFO, 
            filemode = 'w',
            format='%(asctime)s - %(levelname)s - %(message)s'
        )
        self.logger = logging.getLogger(__name__)
        self.logger.info("Inicializando DataProcessor")

        # Carregar variáveis do .env para conexão com o banco
        load_dotenv()
        self.db_user = os.getenv("DB_USER")
        self.db_password = os.getenv("DB_PASSWORD")
        self.db_host = os.getenv("DB_HOST")
        self.db_port = os.getenv("DB_PORT")
        self.db_name = os.getenv("DB_NAME")

        # Carregar configuração do arquivo JSON
        with open(config_path, "r") as f:
            config = json.load(f)

        self.skip_input_gen = config["skip_input_gen"]
        self.skip_grid_gen = config.get("skip_grid_gen", False)  # Garantir existência
        self.input_from_clause = config["input_from_clause"]
        self.grid_from_clause = config["grid_from_clause"]

        # Parâmetro opcional
        if grid_spacing is None:
            self.grid_spacing = config["grid_spacing"]
        else:
            self.grid_spacing = grid_spacing

        self.logger.info(f"Configuração carregada com espaçamento de grid: {self.grid_spacing}")

        self.output_parquet = config["input_file"]
        self.grid_output_parquet = config["grid_file"]

        # Criar o engine de conexão
        self.engine = create_engine(
            f"postgresql://{self.db_user}:{self.db_password}@{self.db_host}:{self.db_port}/{self.db_name}"
        )

    def load_municipio_data(self):
        # Montar a query com base no parâmetro municipios
        query = f"""
        SELECT c.ogc_fid AS id, 
               'CAR' AS id_layer, 
               ST_CollectionExtract(c.geom,3) geom 
        {self.input_from_clause};
        """
        #print(query)
        # Carregar os dados em um GeoDataFrame
        try:
            gdf = gpd.read_postgis(query, self.engine, geom_col="geom")
            if gdf.empty:
                self.logger.warning("Nenhuma geometria encontrada para os municípios especificados.")
                return None
            gdf.set_crs("EPSG:4674", inplace=True)
            self.logger.info("Dados dos municípios carregados com sucesso.")
            return gdf
        except Exception as e:
            self.logger.error(f"Erro ao carregar dados dos municípios: {e}")
            raise ValueError("É necessária a geometria para prosseguir")

    def export_municipio_data(self, gdf):
        try:
            print('Salvando arquivos grids...')
            gdf.to_parquet(self.output_parquet)
            self.logger.info(f"Arquivo Parquet exportado com sucesso para {self.output_parquet}")
            #gdf.to_file(self.output_parquet.replace(".parquet", ".gpkg"), layer='input', driver="GPKG")
            print('Salvo ! ')
        except Exception as e:
            self.logger.error(f"Erro ao exportar dados dos municípios: {e}")

    def create_grid(self):
        
        # Construir a query de grid usando as cláusulas do config.json
        grid_query = f"""
        WITH resultado_municipio AS (
            SELECT geom {self.input_from_clause}
        ),
        envelope AS (
            SELECT geom AS bbox {self.grid_from_clause}
        ),
        grid AS (
            SELECT (ST_SquareGrid({self.grid_spacing}, bbox)).geom AS geom
            FROM envelope
        )
        SELECT row_number() OVER () AS grid_id, geom FROM grid;
        """
        
        #print(grid_query)  # Apenas para debug
        try:
            grid_gdf = gpd.read_postgis(grid_query, self.engine, geom_col="geom")
            if grid_gdf.empty:
                self.logger.warning("Nenhuma célula de grid foi gerada.")
                return None
            grid_gdf.set_crs("EPSG:4674", inplace=True)
            self.logger.info("Grid criado com sucesso.")
            return grid_gdf
        except Exception as e:
            self.logger.error(f"Erro ao criar o grid: {e}")
            return None

    def export_grid_data(self, grid_gdf):
        try:
            print('Salvando arquivos grids...')
            grid_gdf.to_parquet(self.grid_output_parquet)
            self.logger.info(f"Grid Parquet exportado com sucesso para {self.grid_output_parquet}")
            grid_gdf.to_file(self.grid_output_parquet.replace(".parquet", ".gpkg"), layer='grid', driver="GPKG")
            print('Salvo !')
        except Exception as e:
            self.logger.error(f"Erro ao exportar dados do grid: {e}")

    def run(self):
        
        self.logger.info("Iniciando processamento de dados...")
        if not self.skip_input_gen:
            municipio_gdf = self.load_municipio_data()
            if municipio_gdf is not None:
                self.export_municipio_data(municipio_gdf)
        else:
            self.logger.info(f"A flag skip_input_gen foi setada para True, skipping geração de input")

        if not self.skip_grid_gen:
            grid_gdf = self.create_grid()
            if grid_gdf is not None:
                self.export_grid_data(grid_gdf)

        else:
            self.logger.info(f"A flag skip_input_gen foi setada para True, skipping geração de grid")

        self.logger.info("Processamento concluído.")



# # Executa o loop em paralelo
# if __name__ == "__main__":

#     # Define o tempo de início
#     start_time = time.time()

#     # Usa partial para fixar os primeiros três argumentos
#     dataprocessor = DataProcessor(grid_spacing=0.5)
#     dataprocessor.run()


    
#     # Calcula o tempo decorrido
#     elapsed_time = time.time() - start_time
#     print(f'Demorou {elapsed_time:.2f} segundos para rodar tudo')