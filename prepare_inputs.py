import geopandas as gpd
import json
import os
from sqlalchemy import create_engine
from dotenv import load_dotenv
import time

class DataProcessor:
    def __init__(self, config_path="config.json", grid_spacing=None):
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

        
        self.municipios = config["municipio"]

        # Parâmetro opcional
        if grid_spacing is None:
            self.grid_spacing = config["grid_spacing"]
        else:
            self.grid_spacing = grid_spacing

        print(f"Grid spacing: {self.grid_spacing}")
        
        self.output_parquet = config["input_file"]
        self.grid_output_parquet = config["grid_file"]

        # Criar o engine de conexão
        self.engine = create_engine(f"postgresql://{self.db_user}:{self.db_password}@{self.db_host}:{self.db_port}/{self.db_name}")

    def load_municipio_data(self):
 
        # Montar a query com base no parâmetro municipios
        if self.municipios[0] == "all":
            municipio_condition = ""  # Não aplica filtro, pegando todos os municípios
        elif self.municipios[0] != "all":
            municipios_formatted = "', '".join(self.municipios)
            municipio_condition = f"WHERE municipio IN ('{municipios_formatted}')"
        else:
            raise ValueError("O parâmetro 'municipios' deve ser uma lista ou o valor 'all'.")


        query = f"""
        SELECT c.ogc_fid AS id, 
               'CAR' AS id_layer, 
               c.geom  
        FROM car.car c 
        {municipio_condition};
        """
        # Carregar os dados em um GeoDataFrame
        try:
            gdf = gpd.read_postgis(query, self.engine, geom_col="geom")
            if gdf.empty:
                print("Nenhuma geometria encontrada para os municípios especificados.")
                return None
            gdf.set_crs("EPSG:4674", inplace=True)
            return gdf
        except Exception as e:
            print(f"Erro ao carregar dados dos municípios: {e}")
            return None

    def export_municipio_data(self, gdf):
        # Exportar para Parquet e GeoJSON
        gdf.to_parquet(self.output_parquet)
        print(f"Arquivo Parquet exportado com sucesso para {self.output_parquet}")
        
        ##
        gdf.to_file(self.output_parquet.replace(".parquet", ".geojson"), driver="GeoJSON")
        


    def create_grid(self):
        # Montar a condição para a query do grid
        if self.municipios[0] == "all":
            municipio_condition = ""  # Não aplica filtro, pegando todos os municípios
        elif self.municipios[0] != "all":
            municipios_formatted = "', '".join(self.municipios)
            municipio_condition = f"WHERE municipio IN ('{municipios_formatted}')"
        else:
            raise ValueError("O parâmetro 'municipios' deve ser uma lista ou o valor 'all'.")

        grid_query = f"""
        WITH resultado_municipio AS (
            SELECT geom, ogc_fid
            FROM car.car
            {municipio_condition}
        ),
        grid AS (
            SELECT (ST_SquareGrid({self.grid_spacing}, ST_Envelope(ST_Union(geom)))).geom AS geom
            FROM resultado_municipio
        )
        SELECT row_number() OVER () AS grid_id,
            geom
        FROM grid;
        """
        
        try:
            grid_gdf = gpd.read_postgis(grid_query, self.engine, geom_col="geom")
            if grid_gdf.empty:
                print("Nenhuma célula de grid foi gerada. Verifique a extensão ou os dados dos municípios.")
                return None
            grid_gdf.set_crs("EPSG:4674", inplace=True)
            return grid_gdf
        except Exception as e:
            print(f"Erro ao criar o grid: {e}")
            return None

    def export_grid_data(self, grid_gdf):
        # Exportar grid para Parquet e GeoJSON
        grid_gdf.to_parquet(self.grid_output_parquet)
        print(f"Grid Parquet exportado com sucesso para {self.grid_output_parquet}")

                ##
        grid_gdf.to_file(self.grid_output_parquet.replace(".parquet", ".geojson"), driver="GeoJSON")
        

    def run(self):
        # Executa o processo completo de exportação dos dados do município e do grid
        municipio_gdf = self.load_municipio_data()
        if municipio_gdf is not None:
            self.export_municipio_data(municipio_gdf)
        
        grid_gdf = self.create_grid()
        if grid_gdf is not None:
            self.export_grid_data(grid_gdf)


# # Executa o loop em paralelo
# if __name__ == "__main__":

#     # Define o tempo de início
#     start_time = time.time()

#     # Usa partial para fixar os primeiros três argumentos
#     dataprocessor = DataProcessor(grid_spacing=0.25)
#     dataprocessor.run()


    
#     # Calcula o tempo decorrido
#     elapsed_time = time.time() - start_time
#     print(f'Demorou {elapsed_time:.2f} segundos para rodar tudo')