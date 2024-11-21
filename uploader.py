import geopandas as gpd
import os
from sqlalchemy import create_engine, text
import pandas as pd
import numpy as np
from utils import format_array
import time



def upload_parquet(engine, gdf, table_name):
    start_time = time.time()
    gdf=gdf[['id', 'id_layer', 'id_feature', 'geometry']]
    # Aplicando a função
    gdf['id_layer'] = format_array(gdf['id_layer'])
    gdf['id_feature'] = format_array(gdf['id_feature'])
    gdf.to_postgis(
        name=table_name,
        con=engine,
        schema='split',
        if_exists="append",
        index=False
    )
    return time.time() - start_time

def upload_full_folder(engine, folder, table_name):
    arquivos=os.listdir(folder)
    for file_path in arquivos:
        print(f"Uploading {file_path}")
        gdf=gpd.read_parquet(os.path.join(folder, file_path))
        upload_parquet(engine=engine, gdf=gdf, table_name=table_name)