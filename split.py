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
from shapely.strtree import STRtree
from sqlalchemy import text, create_engine
from dotenv import load_dotenv
from sqlalchemy import Table, MetaData, Index




def load_input(input_file):
    # Carregar o arquivo de entrada
    input_gdf = gpd.read_parquet(input_file)    
    print('Input carregado com sucesso !')
    return input_gdf

class Splitter:

    def __init__(self, config_path="config.json"):
        
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

        # Acessando as variáveis carregadas
        self.grid_file = config["grid_file"]
        self.input_file = config["input_file"]
        self.output_path = config["output_path"]
        self.schema = config["schema"]
        self.num_processes = config["num_processes"]
        self.arquivo_final = config["arquivos_final"]
        self.split_table_name = config["split_table_name"]
        self.memory = psutil.virtual_memory()

        # Cria objetos estáticos vazios
        self.grid_gdf = None
        self.input_gdf = None
        self.start_time = time.time()
        
        #Logger dentro do init
        self.logger = logging.getLogger(self.__class__.__name__)
        self.logger.setLevel(logging.DEBUG)                
        # Configuração básica de saída para o console
        file_handler = logging.FileHandler('logs/splitter.log', mode='w', encoding='utf-8')
        formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        file_handler.setFormatter(formatter)
        # Adiciona o handler ao logger
        if not self.logger.hasHandlers():  # Evita duplicação de handlers
            self.logger.addHandler(file_handler)

        # Dicionário com zonas UTM e seus respectivos códigos EPSG (apenas para o Brasil)
        self.utm_epsg_brazil = {
            18: 32718,  # UTM Zona 18S
            19: 32719,  # UTM Zona 19S
            20: 32720,  # UTM Zona 20S
            21: 32721,  # UTM Zona 21S
            22: 32722,  # UTM Zona 22S
            23: 32723,  # UTM Zona 23S
            24: 32724,  # UTM Zona 24S
            25: 32725   # UTM Zona 25S
        }



        self.logger.info(f"Splitter instanciado com sucesso")



    
    def _intersection_sql(self, n_grid, grid_gdf, engine):
        """
        Realiza uma consulta SQL para selecionar geometrias que intersectam a unidade_split.
        
        Args:
            engine de conexão com o banco;
            n_grid - número do grid para o qual será feito o processamento
            grid_gdf - tabela com todos os grids para selecionar pelo número dado. Essa tabela é inputada para não ficar instanciada na memoria

        Returns:
            Retorno é o elapsed_time, mas um objeto é criado dentro da instância
        """
        start_time=time.time()

        #Unidade split é o grid em questão. O split é performado apenas entre as feicoes que tocam o grid.
        self.n_grid = n_grid
        self.unidade_split = grid_gdf[grid_gdf["id"] == self.n_grid].geometry.values[0]

        # Garantir que unidade_split esteja definida
        if not hasattr(self, "unidade_split"):
            self.logger.error("unidade_split não está definida.")
            raise ValueError("unidade_split precisa estar definida antes de chamar intersection_sql.")
        
        # Extrair os bounds da unidade_split
        bounds = self.unidade_split.bounds  # (minx, miny, maxx, maxy)
        minx, miny, maxx, maxy = bounds
        
        # Criar a query SQL para filtrar na tabela inputs apenas os registros que estao no bounding box do grid
        query = f"""
        SELECT id, id_layer, geom
        FROM {self.split_table_name}
        WHERE geom && ST_MakeEnvelope({minx}, {miny}, {maxx}, {maxy}, 4674
        );
        """
        
        # Conectar ao banco de dados e ler
        try:      
               
            # Executar a consulta e carregar os dados como GeoDataFrame
            result_gdf = gpd.read_postgis(query, con=engine, geom_col='geom')

            # Criar o GeoDataFrame final no formato desejado
            self.gdf_input_intersection = gpd.GeoDataFrame(data={
                    'id': result_gdf['id'],
                    'id_layer': result_gdf['id_layer'],
                    'geom': result_gdf.geom},
                geometry='geom',
                crs='EPSG:4674'
            )

            #Indexa o GeoDF
            self.spatial_index = STRtree(self.gdf_input_intersection.geom)            
            elapsed_time=time.time()-start_time            
            return f'{elapsed_time:.2f}'
        
        #Erro genérico (ponto de melhoria)
        except Exception as e: 
            self.logger.error(f"Erro ao executar consulta SQL: {e}")
            raise
  
    def prepare_split_line(self):
        
        self.counter=0
        start_time=time.time()

        """Essa funcao é a mais complicada do código
        O que ela se propõe a fazer é simples: Gerar uma MultiLinestring que será inputada no shp.node()
        Essa multilinestring deve ser construída da maneira mais manual possível"""

        try:
            linerings=[]
            #Temos que forçar que todos os poligonos de entrada devem formar lines rings. Caso isso não seja possível a geometria deve ser descartada pois vai dar BO
            for index, row in self.gdf_input_intersection.iterrows():
                #Seleciona geometria do dado
                geom=row.geom           
                #Se for multipolygon, seleciona o primeiro polygon da feição e pega o boundary. 
                # Aqui todas as geometria sao validas
                if isinstance(geom, MultiPolygon):   
                    geom= geom.geoms[0] 
                    line=geom.exterior
                elif isinstance(geom, Polygon):
                    geom = geom
                    line=geom.exterior # A funcao exterior é um sacada, ao invés de usar a boundary. Ler documentacao para compreender.

                #Se a linha capturada for válida, appenda na lista de linearRings, se não for válida, joga no lixo pelo bem da humanidade
                if line.is_valid:
                    line=LinearRing(line)
                    linerings.append(line)
                else:
                    self.counter+=1
                    #print(f"Feição descartada - ID: {row['id']}, ID Layer: {row['name']}, Geometria: {geom}")
                    #passa pro promixo loop e nao appenda
                    continue   

            #Esse try é crítico. As tres próximas linhas são onde mais ocorre erro, principalmente a função node que ainda é um certo mistério
            # como funciona. 
            try:            
                #Appenda o grid, para que seja feita a reconstrucao total do grid
                linerings.append(LinearRing(self.unidade_split.exterior))
                #Cria um MultiLineString a partir de todas as linhas
                multi_line=MultiLineString(linerings)
                #Cria o MultiLineString com nós onde as linhas se cruzam
                self.multi_line_with_nodes=shp.node(multi_line)        
            except Exception as e:
                #Caso ocorra algum exception, o grid é pulado
                logging.error(f'Nâo foi possivel formar o MultiLinestring pelo motivo {e}')

        except Exception as e:
            logging.error(f'Função prepare_split_line na iteração {self.n_grid} deu o problema {e}')

        elapsed_time=time.time()-start_time
        return f'{elapsed_time:.2f}'

    def perform_split(self):
        
        # Inicia o cronômetro para a operaçãor  
        start_time = time.time()

        try:
            # Dividir o polígono usando a MultiLine com nodes. Forma-se o broken ou shaterred glass
            broken_glass_polygon = list(polygonize(self.multi_line_with_nodes))
            
            
            # Filtra apenas os polígonos cujo representative_point intersecta unidade_split.
            filtered_polygons = [
                poly for poly in broken_glass_polygon 
                if poly.representative_point().intersects(self.unidade_split)
            ]
            
            
            self.gdf_broken_glass = gpd.GeoDataFrame(data={"id": range(1, len(filtered_polygons) + 1)}, 
                                                    geometry=filtered_polygons, crs="EPSG:4674")
            #elapsed_time = time.time() - operation_start
            #self.logger.info(f"Glass shattering complete, levou {elapsed_time:.2f} segundos para o clip do grid {self.n_grid}!")
            del self.multi_line_with_nodes

        except Exception as e:
            logging.error(f'Função perform_split na iteração {self.n_grid} deu o problema {e}')

        #Até aqui tudo muito rápido
        
        elapsed_time=time.time()-start_time
        return f'{elapsed_time:.2f}'


    #O processamento de overlapping é o que mais foi trabalho ate'agora, para tentar minimzar o custo computacional desse procedimento
    # A natureza do processa é custosa, pois, para cada caco de vidro é necessário calcular a quais poligonos originais ele se sobrepõe,
    # Para que seja possivel capturar as informações relacionadas ao poligono. Sem isso, os cacos de vidro ficam se informação na tabela de attr

    def process_overlapping(self):
        """
        Processa todos os fragmentos de vidro sequencialmente.
        Atualiza as colunas 'id_layer' e 'id_feature' no GeoDataFrame self.gdf_broken_glass.
        """
        start_time=time.time()

        # Adiciona a coluna representative_point se ainda não existir. Representative point é um ponto seguro dentro da geometria do caco
        # Importante pois algumas geometria sao muito micro e a funcao centroid da problema.
        if "representative_point" not in self.gdf_broken_glass.columns:
            self.gdf_broken_glass["representative_point"] = self.gdf_broken_glass.geometry.apply(lambda x: x.representative_point())
  
        # Processa cada linha sequencialmente com a funcao self._process_overlap_row (ver a seguir)
        # Essa funcao retorna listas com os ids que devem ser atribuidos a feição
        # Exemplo: Caco de vidro X tem sobreposicao com o CAR 1, 2 e 3. A funcao retorna essa lista [1,2,3].
        
        results = self.gdf_broken_glass.apply(self._process_overlap_row, axis=1)
        
        # Atualiza o GeoDataFrame com os resultados
        self.gdf_broken_glass["id_layer"] = results.apply(lambda x: x[1])
        self.gdf_broken_glass["id_feature"] = results.apply(lambda x: x[2])

        # Remove a coluna de ponto representativo, se não for mais necessária
        self.gdf_broken_glass.drop(columns="representative_point", inplace=True, errors='ignore')
        
        elapsed_time=time.time()-start_time
        return f'{elapsed_time:.2f}'
   
    def _process_overlap_row(self, row):
        """
        Processa uma única linha do GeoDataFrame para encontrar os polígonos mais próximos.
        :param row: Linha do GeoDataFrame.
        :return: Tupla com índice, id_layers e id_features.
        """
        # A funcao lamba aplica essa funcao para cada linha do GDF. Seguimos com isso em mente.
        idx, shard_data = row.name, row

        #Seleciona o ponto
        glass_shard_point = shard_data["representative_point"]

        # Utiliza o indice para buscar o NEAREST. Existem pontos que nao se sobrepõe mas que são selecionados aqui
        # Fazer em duas etapas é mais otimizado, pois 'query nearest é leve e torna o geom.intersects leve pela baixa quantidade de poligonos
        nearest_index = self.spatial_index.query_nearest(glass_shard_point)

        #Seleciona os nearest dentre os poligonos originais de inputs (exemplo CARs originais para CAR split)
        nearest_polygon = self.gdf_input_intersection.iloc[nearest_index]

        #Resetar indice é importante
        nearest_polygon.reset_index(drop=False, inplace = True)

        idx_true_intersection = []
        #Etapa de maior consumo de processador e memoria. Deve ser feito em iteração sequencial. Cada polígono do CAR deve ser
        # testado para sobreposicao com o representative point (glass_shard_point)
        for idx, row in nearest_polygon.iterrows():
            
            if row.geom.intersects(glass_shard_point):
                
                idx_true_intersection.append(idx)
        #Por isso precisa resetar indice
        true_intersection = nearest_polygon.iloc[idx_true_intersection]        

        # Se estiver vazio, retorna apenas id_do grid e id_layer="GRID". Podem ocorrer grids vazios a depender do INPUT.
        # Se não, retorna a lista com os id envolvidos
        if not true_intersection.empty:
            id_layers = ['GRID'] + true_intersection["id_layer"].tolist()
            id_features = [self.n_grid] + true_intersection["id"].tolist()
        else:
            id_layers = ['GRID']
            id_features = [self.n_grid]

        return idx, id_layers, id_features
    


    def colunas_boleanas(self, engine):
        """
        Pega colunas booleanas da tabela de input. Importante para splits com muitas camadas de entrada.
        Exemplo: O split envolva CAR e terras indigenas. Essa funcao cria colunas no dado de saída que serão: is_car e is_ti 
        Com isso, é possivel assignar true ou false nessas colunas para melhorar usabilidade do dado. 
        is_ti = TRUE significa que aquele caco de vidro tem sobrep. com uma TI
        """
        try:
            query=f'select distinct id_layer from {self.split_table_name};'
            df=pd.read_sql_query(query,con=engine)
            boleanas=[i for i in df.id_layer]
            logging.info(f"Colunas booleanas capturadas com sucesso ({boleanas})")
        except Exception as e:
            logging.error(f'Erro na captura das colunas boleanas, não é possivel continuar. ({e})')
            raise

        return boleanas

    def create_table_postgresql(self, engine):
        """
        Cria a tabela no banco de dados. Se não conseguir criar, raise !
        """
        try:
            self.boleanas = self.colunas_boleanas(engine=engine)
            create_query=[f"CREATE SCHEMA IF NOT EXISTS {self.schema};",
                      f"DROP TABLE IF EXISTS {self.schema}.{self.arquivo_final};"]

            #Algumas gambiarras aqui
            
            tabela = f"CREATE TABLE IF NOT EXISTS {self.schema}.{self.arquivo_final} (gid serial, id_layer text[], id_feature integer[], cd_mun integer, cd_uf integer, n_car INTEGER, " + ", ".join([f"is_{x} BOOLEAN" for x in self.boleanas]) + ", area_ha NUMERIC, geometry geometry(polygon, 4674));"
            create_query.append(tabela)

            #Executa as queries na lsita create_query
            with engine.connect() as conn:
                logging.info(f"Criando tabela ({self.arquivo_final}) de saída")
                for q in create_query:
                    with conn.begin():
                        conn.execute(text(q))

            logging.info(f"Tabela {self.arquivo_final} criada com sucesso")

        except Exception as e:
            logging.error(f"Erro na criação da tabela {self.arquivo_final}, interrompendo processo.")

            raise



        return None

    def create_indices(self, engine):
        """
        Cria índices em todas as colunas da tabela self.arquivo_final.
        Utiliza GIST para colunas de geometria.
        """    

        tabela = f"{self.schema}.{self.arquivo_final}"
        colunas = ["cd_mun", "cd_uf", "n_car", "area_ha"] + [f"is_{x}" for x in self.boleanas]

        with engine.connect() as conn:
            for coluna in colunas:
                idx = f"CREATE INDEX idx_{self.arquivo_final}_{coluna} ON {tabela} ({coluna});"                
                with conn.begin():
                    logging.info(idx)
                    conn.execute(text(idx))
  
        
        # Índice GIST para geometria
        idx_geometry = f"CREATE INDEX idx_{self.arquivo_final}_geometry_gist ON {tabela} USING GIST (geometry);"
        with engine.connect() as conn:
            with conn.begin():
                logging.info(idx_geometry)
                conn.execute(text(idx_geometry))

        return None

    def format_gdf_broken_glass(self, n_grid, drop_only_grid=True):
        """
        Essa funcao precisa ser melhor pensada, pois aqui é o momento de facilitar as queries. Então, em cada rodada é bom poder 
        manipular livremente a saída.

        Formata o gdf broken glass, operações:
        1. Dropa coluna ID
        2. Formata os campos id_layer e id_feature para adequação ao db
        3. Cria colunas booleanas e testa o campo id_layer para presenca da camada, retornando true ou false.
        4. Calcula área das feicoes, apenas se drop_only_grid = True (default). Se for falso, mantem as feicoes id_layer=['GRID']
        5. (TESTE) Incluir coluna com cd_uf e cd_mun  
        """

        

        start_time=time.time()
        try:

            #Só processa se id_layer!=['GRID']

            #1. No banco existirá a coluna gid serial para cada feicao inserida. Portanto, dropa a coluna id 
            self.gdf_broken_glass.drop(columns='id', inplace=True)

            #5. Dropar registros em que há apenas a classe 'GRID' no id_layer. Considerando que, sempre deve haver um municipio,
            # um registro apenas com a feicao GRID está fora do Brasil. Se for rodar um split sem municipio, colocar drop_only_grid = False
            if drop_only_grid:
                self.gdf_broken_glass = self.gdf_broken_glass[self.gdf_broken_glass['id_layer'].apply(lambda x: 'MUN' in x)]
            
            # Inserir coluna cd_mun
            self.gdf_broken_glass['cd_mun'] = self.gdf_broken_glass.apply(lambda row: row['id_feature'][row['id_layer'].index('MUN')], axis=1)
            
                      
            #Inserir coluna cd_uf
            self.gdf_broken_glass['cd_uf'] = self.gdf_broken_glass['cd_mun'].astype(str).str[:2].astype(int)


            
            #Contagem de CARs
            self.gdf_broken_glass['n_car'] = np.array([x.count('CAR') for x in self.gdf_broken_glass['id_layer']])

            #2. Aplicando a função para transformar lista em string exemplo [CAR, CAR, GRID] em '{CAR, CAR, GRID}' que é interpretada como array no banco
            self.gdf_broken_glass['id_layer'] = self.gdf_broken_glass['id_layer'].apply(lambda x: '{' + ','.join(map(str, x)) + '}')
            self.gdf_broken_glass['id_feature'] = self.gdf_broken_glass['id_feature'].apply(lambda x: '{' + ','.join(map(str, x)) + '}')

            #3. Cria colunas boleanas
            for coluna in self.boleanas:
                self.gdf_broken_glass[f'is_{coluna.lower()}']=self.gdf_broken_glass['id_layer'].apply(lambda x: f'{coluna}' in x)


            #4. Calcula área das feicoes. Para isso é necessario descobrir a zona do grid e reprojar e dado de acordo com a feicao
            #Descobre em qual zona está o grid
            xmin, ymin, xmax, ymax = self.unidade_split.bounds
            longitude_media_grid=(xmin + xmax) / 2
            zona = int((longitude_media_grid + 180) / 6) + 1
            #Seleciona o epsg correspodente a zona
            projecao = self.utm_epsg_brazil[zona]
            #Reprojeta apenas para calcular area. No entanto a feicao no banco estará em 4674.
            gdf_proj = self.gdf_broken_glass.to_crs(epsg=projecao)
            gdf_proj['area_ha'] = gdf_proj.geometry.area/10000
            #Inputa area na tabela
            self.gdf_broken_glass['area_ha']=gdf_proj['area_ha']
            
            
            #Libera memoria
            del gdf_proj
            del self.unidade_split

        except Exception as e:
            logging.error(f"Erro na formatação do output para a iteração {n_grid} não é possivel continuar ({e})")
            

        elapsed_time=time.time()-start_time
        return f'{elapsed_time:.2f}'

    def upload_db(self, engine):
        memory = psutil.virtual_memory()
        cpu_percent = psutil.cpu_percent(interval=0.1)  
        start_time = time.time()         
        
        #Upload direto no db
        self.gdf_broken_glass.to_postgis(
            name=self.arquivo_final,
            con=engine,
            schema=self.schema,
            if_exists="append",
            index=False
        )

        self.logger.info(f"Iteração do grid {self.n_grid} armazenada - Uso de memória : {memory.percent}% - CPU : {cpu_percent}%")
        del self.gdf_broken_glass
        elapsed_time=time.time()-start_time
        return f'{elapsed_time:.2f}'

    def run(self, n_grid, grid_gdf):
        # Função que processa cada grid específico
        
        start_time=time.time()
        try:

            # Criar o engine dentro de cada processo
            engine = create_engine(
                f"postgresql://{self.db_user}:{self.db_password}@{self.db_host}:{self.db_port}/{self.db_name}"
            )

            
            intersection_time=self._intersection_sql(n_grid=n_grid, grid_gdf=grid_gdf, engine=engine)
            
            prepare_lines_time=self.prepare_split_line()
            perform_split_time=self.perform_split()
            
            overlapping_time=self.process_overlapping()
            
            format_gdf=self.format_gdf_broken_glass(n_grid=n_grid)
           
            upload_time=self.upload_db(engine=engine) #Inserir isso como método na classe
            elapsed_time=time.time()-start_time
            
            tempos={'intersection_time':intersection_time,
                    'prepare_lines_time':prepare_lines_time,
                    'perform_split_time':perform_split_time,
                    'overlapping_time':overlapping_time,
                    'format_gdf':format_gdf,
                    'upload_sql_time':upload_time}
            
            
            # Tratar valores None e converter para float
            tempos_cleaned = {k: float(v) if v is not None else 0.0 for k, v in tempos.items()}

            # Encontrar o maior tempo e a chave correspondente
            max_time_func, max_time_value = max(tempos_cleaned.items(), key=lambda item: item[1])
            #Encerra conexão, muito importante !!
            engine.dispose()
            
            logging.info(f'Iteração completa para o {n_grid} levou {elapsed_time:.2f} e a operação que levou mais tempo foi a funcao {max_time_func} com {max_time_value} e descartou {self.counter} feicoes')
            logging.info(f'Tempos: {tempos}')
            

        #Se der erro prossegue 
        except Exception as e:
            # Registra o n_grid no arquivo de erro e no log o erro que ocorreu
            with open("logs/error_grids.txt", "a") as error_file:
                error_file.write(f"{n_grid}\n")
            self.logger.error(f"Iteração do grid {self.n_grid} ERRO {e}")
            
        
    def run_parallel(self, grids, grid_gdf):
        #Essa funcao cria diversas instancias da Classe
        run_splitter_partial = partial(self.run, grid_gdf=grid_gdf)
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


 
