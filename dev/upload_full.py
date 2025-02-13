import logging
import sys
sys.path.append('/home/pedro/Documents/GIT_WORKSPACE/split_offdb')
from multiprocessing import Pool
from split import Splitter, load_input
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
from rtree import index
from utils import merge_parquet_files
from sqlalchemy import text, create_engine
from uploader import upload_parquet, upload_full_folder
from dotenv import load_dotenv


with open('config.json', 'r') as f:
    config=json.load(f)



db_user = os.getenv("DB_USER")
db_password = os.getenv("DB_PASSWORD")
db_host = os.getenv("DB_HOST")
db_port = os.getenv("DB_PORT")
db_name = os.getenv("DB_NAME")
#Criar o engine de conexão
engine = create_engine(
f"postgresql://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}"
)


#criar schema e tabela
create_query=f"CREATE SCHEMA IF NOTE EXISTS split;CREATE TABLE IF NOT EXISTS split.{config['arquivos_final']} (id integer, id_layer text[], id_feature integer[], geometry geometry(polygon, 4674))"
with engine.connect() as conn:
    with conn.begin():
        r=conn.execute(text(create_query))


start_time=time.time()
upload_full_folder(engine=engine, folder=config['output_path'], table_name=config['arquivos_final'])
print(f'Elapsed time para upload {(time.time() - start_time):.2s}')

queries=['create index if not exists on split.split (id);',
         'create index if not exists on split.split (id_layer);',
         'create index if not exists on split.split using gist (geom);',
         'create index if not exists on split.split (id_feature);']


with engine.connect() as conn:
    for q in queries:
        with conn.begin():
            r=conn.execute(text(q))