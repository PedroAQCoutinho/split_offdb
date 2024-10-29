import geopandas as gpd
import json
from sqlalchemy import create_engine

# Carregar configurações do config.json
with open("config.json", "r") as f:
    config = json.load(f)

# Configurações do banco de dados
db_user = config["db_user"]
db_password = config["db_password"]
db_host = config["db_host"]
db_port = config["db_port"]
db_name = config["db_name"]
municipio = config["municipio"]
grid_spacing = config["grid_spacing"]

# Caminhos de saída
output_parquet = config["output_parquet"]
output_geojson = config["output_geojson"]
grid_output_parquet = config["grid_output_parquet"]
grid_output_geojson = config["grid_output_geojson"]

# Criação do engine de conexão
engine = create_engine(f"postgresql://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}")

# Query para seleção de dados do município
query = f"""
SELECT c.ogc_fid AS id, 
       'CAR' AS id_layer, 
       c.geom  
FROM car.car c 
WHERE municipio = '{municipio}';
"""

# Executar a query e carregar os dados em um GeoDataFrame
try:
    gdf = gpd.read_postgis(query, engine, geom_col="geom")
    print(query)
    
    # Verificar se o GeoDataFrame tem dados
    if gdf.empty:
        print("Nenhuma geometria encontrada para o município especificado.")
    else:
        # Definir o CRS explicitamente (EPSG:4674)
        gdf.set_crs("EPSG:4674", inplace=True)
        
        # Exportar o GeoDataFrame para Parquet
        gdf.to_parquet(output_parquet)
        print(f"Arquivo Parquet exportado com sucesso para {output_parquet}")

        # Exportar o GeoDataFrame para GeoJSON
        gdf.to_file(output_geojson, driver="GeoJSON")
        print(f"Arquivo GeoJSON exportado com sucesso para {output_geojson}")

        # Query para criação do grid utilizando ST_SquareGrid com ST_Envelope
        grid_query = f"""
        WITH resultado_municipio AS (
            SELECT geom, ogc_fid
            FROM car.car
            WHERE municipio = '{municipio}'
        ),
        grid AS (
            SELECT (ST_SquareGrid({grid_spacing}, ST_Envelope(ST_Union(geom)))).geom AS geom
            FROM resultado_municipio
        )
        SELECT row_number() OVER () AS grid_id,
            geom
        FROM grid;
        """
        
        # Executar a query para o grid e carregar o resultado em um GeoDataFrame
        grid_gdf = gpd.read_postgis(grid_query, engine, geom_col="geom")
        
        if grid_gdf.empty:
            print("Nenhuma célula de grid foi gerada. Verifique a extensão ou os dados do município.")
        else:
            # Definir o CRS explicitamente para o grid (EPSG:4674)
            grid_gdf.set_crs("EPSG:4674", inplace=True)
            
            # Exportar o grid para Parquet
            grid_gdf.to_parquet(grid_output_parquet)
            print(f"Grid Parquet exportado com sucesso para {grid_output_parquet}")

            # Exportar o grid para GeoJSON
            grid_gdf.to_file(grid_output_geojson, driver="GeoJSON")
            print(f"Grid GeoJSON exportado com sucesso para {grid_output_geojson}")

except ImportError as e:
    print("Erro: pyarrow não está instalado. Instale-o usando 'pip install pyarrow' e tente novamente.")
except Exception as e:
    print(f"Ocorreu um erro: {e}")
