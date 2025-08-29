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
from prepare_inputs import DataProcessor



# def load_input(input_file):
#     # Carregar o arquivo de entrada
#     input_gdf = gpd.read_parquet(input_file)    
#     print('Input carregado com sucesso !')
#     return input_gdf

class Splitter():

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
        #GRID
        self.grid_schema = config['grid']['schema']
        self.grid_nome = config['grid']['nome']
        #OUTPUT 
        self.output_schema = config['output']["schema"]
        self.output_nome = config['output']["tabela_saida"]
        #INPUT
        self.input_schema = config['input_algoritmo_split']['schema']
        self.input_name = "input_" + self.output_nome
        #VISAO FUNDIARIA
        self.has_join = config['tabela_visao_fundiaria']['has_join']
        self.campos = config['tabela_visao_fundiaria']['nome_coluna_visao_fundiaria']
        self.coluna_hexadecimal = config['tabela_visao_fundiaria']['nome_coluna_hexadecimal']
        self.path_visao_fundiaria = config['tabela_visao_fundiaria']['path_arquivo_csv']

        self.num_processes = config["config"]["num_processes"]
        
        #schema + input_ + tabela_saida = nome da tabela de entrada
        self.split_table_name = f'{config["output"]["schema"]}.input_{config["output"]["tabela_saida"]}'

        self.memory = psutil.virtual_memory()

        # Cria objetos estáticos vazios
        self.grid_gdf = None
        self.input_gdf = None
        self.start_time = time.time()
        

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


        
    def _intersection_sql(self, n_grid, engine):
        """
        Realiza uma consulta SQL para selecionar geometrias que intersectam a unidade_split.
        
        Args:
            engine de conexão com o banco;
            n_grid - número do grid para o qual será feito o processamento
            grid_gdf - Tabela com todos os grids para selecionar pelo número n_grid. Essa tabela é inputada para não ficar instanciada na memoria

        Returns:
            Retorno é o elapsed_time, mas um objeto é criado dentro da instância
        """
        start_time=time.time()

        #Unidade split é o grid em questão. O split é performado apenas entre as feicoes que tocam o grid.
        self.n_grid = n_grid

        query = f"""
            SELECT geom
            FROM {self.grid_schema}.{self.grid_nome}
            WHERE id = {self.n_grid};
            """
        #Extrai o poligono do banco
        grid_gdf = gpd.read_postgis(query, con=engine, geom_col='geom')
        #Unidade split
        self.unidade_split = grid_gdf.geometry.values[0]


        # Garantir que unidade_split esteja definida
        if not hasattr(self, "unidade_split"):
            logging.error("unidade_split não está definida.")
            raise ValueError("unidade_split precisa estar definida antes de chamar intersection_sql.")
        
        
        # Criar a query SQL para filtrar na tabela inputs apenas os registros que estao no bounding box do grid
        query = f"""
        select a.id, a.id_layer, a.hexadecimal, a.geom
        from {self.input_schema}.{self.input_name} a
        where a.geom && (
            select st_envelope(geom)
            from {self.grid_schema}.{self.grid_nome}
            where id = {self.n_grid}
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
                    'hexadecimal' : result_gdf['hexadecimal'],
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
            logging.error(f"Erro ao executar consulta SQL: {e}")
            raise
  
    def prepare_split_line(self):
        
        self.counter=[]
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
                #Nova versão, aqui, para multipolygons ele explode a feicao e captura todos os subpolygons. É importante pois se não seriam perdidos fragmentos do multipolygon
                #Extrair os poligonos de multipoligons ou pega a geometria in natura caso seja diferente de multipolygon
                polys = list(geom.geoms) if isinstance(geom, MultiPolygon) else [geom]
                for poly in polys:
                    #Aqui, só serão admitidas entradas polygon, caso contrário, continue (descarte)
                    if not isinstance(poly, Polygon):
                        continue
                    # exterior
                    ext = poly.exterior
                    if ext and ext.is_valid:
                        linerings.append(LinearRing(ext.coords))
                    # interiores (buracos)
                    # Os buracos interiores devem ser também declarados como geometrias, e uma vez feito o split, elas não se sobreporão a nada
                    for hole in poly.interiors:
                        if hole and LinearRing(hole.coords).is_valid:
                            linerings.append(LinearRing(hole.coords))

            #Esse try é crítico. As tres próximas linhas são onde mais ocorre erro, principalmente a função node/unary_union que ainda é um certo mistério
            # de como funciona. 
            #Substitui node por unary_union. Nao lembro pq
            try:            
                #Appenda o grid, para que seja feita a reconstrucao total do grid
                linerings.append(LinearRing(self.unidade_split.exterior))
                #Cria um MultiLineString a partir de todas as linhas
                multi_line=MultiLineString(linerings)
                #Cria o MultiLineString com nós onde as linhas se cruzam
                self.multi_line_with_nodes=shp.unary_union(multi_line)        
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
            #logging.info(f"Glass shattering complete, levou {elapsed_time:.2f} segundos para o clip do grid {self.n_grid}!")
            del self.multi_line_with_nodes
            self.broken_glass_polygon = broken_glass_polygon

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
        self.gdf_broken_glass["hexadecimal"] = results.apply(lambda x: x[3])
        

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
        # Fazer em duas etapas é mais otimizado, pois 'query nearest() é leve e torna o geom.intersects leve pela baixa quantidade de poligonos
        # Ajuste 28-ago: Descobri que o query nearest pode deixar de retornar algo que deveria, então teremos que usar o query() aqui
        cand_idx = self.spatial_index.query_nearest(glass_shard_point)
        if isinstance(cand_idx, (int, np.integer)):
            cand_idx = [cand_idx]

        #Seleciona os nearest dentre os poligonos originais de inputs (exemplo CARs originais para CAR split)
        nearest_polygon = self.gdf_input_intersection.iloc[cand_idx].copy()

        #Resetar indice é importante
        nearest_polygon.reset_index(drop=False, inplace = True)

        idx_true_intersection = []
        #Etapa de maior consumo de processador e memoria. Deve ser feito em iteração sequencial. Cada polígono do CAR deve ser
        # testado para sobreposicao com o representative point (glass_shard_point)
        for idx, row in nearest_polygon.iterrows():
            try:
                #Contains é mais robusto que intersects por questões de margem
                if row.geom.contains(glass_shard_point):
                    
                    idx_true_intersection.append(idx)
            except Exception as e:
                    #Tenta novamente torná-lo válido
                    valid_polygon = row.geom.buffer(0)
                    
                    if valid_polygon.contains(glass_shard_point):
                    
                        idx_true_intersection.append(idx)


        #Por isso precisa resetar indice
        true_intersection = nearest_polygon.iloc[idx_true_intersection]   

        # Se estiver vazio, retorna apenas id_do grid e id_layer="GRID". Podem ocorrer grids vazios a depender do INPUT.
        # Se não, retorna a lista com os id envolvidos
        if not true_intersection.empty:
            # old
            # id_layers = ['GRID'] + true_intersection["id_layer"].tolist()
            # id_features = [self.n_grid] + true_intersection["id"].tolist()
            # hexadecimal = true_intersection["hexadecimal"].sum()

            #Sugestão chatGPT para padronizar o tipo de array. Importante na hora de subir as coisas no DB
            id_layers   = ['GRID'] + true_intersection["id_layer"].astype(str).str.upper().tolist()
            id_features = [int(self.n_grid)] + [int(x) for x in true_intersection["id"].tolist()]
            # soma segura:
            # O GRID nativamente nao tem hexadecimal, então aqui ele surge como NA. 
            # Ao usar fillna(0) atribuimos ao grid sempre 0, o que nao intefere
            # únicos por valor de hexadecimal
            hex_vals = (
                pd.to_numeric(true_intersection["hexadecimal"], errors="coerce")
                .dropna()
                .astype("int")
                .unique()
            )
            hexadecimal = int(np.sum(hex_vals)) if hex_vals.size else 0
            
            
        else:
            id_layers = ['GRID']
            id_features = [int(self.n_grid)]
            hexadecimal = 0

        return idx, id_layers, id_features, hexadecimal
    
    #Formatação
    def colunas_boleanas(self, engine):
        """
        Pega colunas booleanas da tabela de input. Importante para splits com muitas camadas de entrada.
        Exemplo: O split envolva CAR e terras indigenas. Essa funcao cria colunas no dado de saída que serão: is_car e is_ti 
        Com isso, é possivel assignar true ou false nessas colunas para melhorar usabilidade do dado. 
        is_ti = TRUE significa que aquele caco de vidro tem sobrep. com uma TI
        """
        try:
            query=f'select distinct id_layer from {self.input_schema}.{self.input_name};'
            df=pd.read_sql_query(query,con=engine)
            boleanas=[i for i in df.id_layer]
            logging.info(f"Colunas booleanas capturadas com sucesso ({boleanas})")
        except Exception as e:
            logging.error(f'Erro na captura das colunas boleanas, não é possivel continuar. ({e})')
            raise

        return boleanas

    #Define base_cols
    def array_base_cols(self):
        """Define quais serão as colunas da tabela saída do modelo"""
        bc = {
                'gid'            : 'serial PRIMARY KEY',
                'id_layer'       : 'text[]',
                'id_layer_unico' : 'text[]',
                'id_feature'     : 'integer[]',
                'hexadecimal'    : 'bigint',
                'cd_mun'         : 'integer',
                'cd_uf'          : 'integer',
                'n_car'          : 'integer',
                'area_ha'        : 'numeric(20,4)',
                # booleanas entram na sequencia
                # geometry entra por último
            }

        #Adiciona as colunas booleanas
        for x in self.boleanas:
            bname = f'is_{str(x).lower()}'
            bc[bname] = 'boolean'
        #Adiciona os campos da tabela de visões 
        if self.has_join:                
            if not len(self.campos)==0:
                for campo in self.campos:
                    bc[campo] = 'text'
        #Adiciona a coluna geom
        bc['geometry'] = 'geometry(Polygon, 4674)'


        return bc

    #Auxiliar
    def create_table(self, engine):
        """
        Cria a tabela no banco de dados. Se não conseguir criar, raise !
        """         
        self.boleanas = self.colunas_boleanas(engine=engine)
        create_query=[f"CREATE SCHEMA IF NOT EXISTS {self.output_schema};",
                    f"DROP TABLE IF EXISTS {self.output_schema}.{self.output_nome};"]
        
        #Cnostroi o dictionary com as colunas base
        base_cols=self.array_base_cols()

        #Querie final para criação da tabela
        tabela = f"""CREATE TABLE IF NOT EXISTS 
        {self.output_schema}.{self.output_nome} (""" +  ", ".join([f"{x} {base_cols[x]}" for x in base_cols.keys()]) + ')'  
            
        #Lista com sequencia de 3 queries (CREATE SCHEMA, DROP IF EXISTES, CREATE TABLE)
        create_query.append(tabela)
        
        with engine.begin() as conn:
            for q in create_query:
                try:
                    conn.execute(text(q))
                    # Coleta resultados apenas quando houver linhas (SELECT)                    
                    logging.info(f"Query executada com sucesso. \n {q}")
                    # NUNCA chame conn.commit() aqui
                except Exception as e:
                    # logger com stacktrace
                    logging.exception(f"Erro executando query: {q[:200]} ...")
                    raise  # deixa a exception subir (útil p/ quem chamou decidir)


            logging.info(f"Tabela {self.output_nome} criada com sucesso")

        return None

    #Auxiliar
    def create_indices(self, engine):
        """
        Cria índices em todas as colunas da tabela self.output_nome.
        Utiliza GIST para colunas de geometria.
        """    
        #Dicionario de colunas
        base_cols=self.array_base_cols()
        tabela = f"{self.output_schema}.{self.output_nome}"
        colunas = base_cols.keys()

        with engine.begin() as conn:
            for coluna in colunas:
              
                alias = "USING GIST" if coluna == "geometry" else ""
                idx = f"CREATE INDEX idx_{self.output_schema}_{self.output_nome}_{coluna} ON {tabela} {alias} ({coluna});" 

                try:
                    conn.execute(text(idx))
                    # Coleta resultados apenas quando houver linhas (SELECT)                    
                    logging.info(f"Query executada com sucesso. \n {idx}")
                    # NUNCA chame conn.commit() aqui
                except Exception as e:
                    # logger com stacktrace
                    logging.exception(f"Erro executando query: {idx[:200]} ...")
                    raise  # deixa a exception subir (útil p/ quem chamou decidir)


  

        return None

    #Formatação
    def format_gdf_broken_glass(self):
        """
        Essa funcao precisa ser melhor pensada, pois aqui é o momento de facilitar as queries. Então, em cada rodada é bom poder 
        manipular livremente a saída.

        Formata o gdf broken glass, operações:
        # 1. Dropa coluna id pq no banco ja existe id serial4
        # 2. Drop onde é id_layer = ['GRID'] através da condiução hexadecimal = 0. O objetivo é descartar geometrias que não tem nenhuma informação
        # 3. Adiciona colunas cd_mun e cd_uf
        # 4. Conta número de CARs na feição
        # 5. Adiciona colunas boleanas
        # 6. Calcula área das feicoes. Para isso é necessario descobrir a zona do grid e reprojar e dado de acordo com a feicao
        # 7. Cria a coluna id_layer_unico
        # 8. Converte as arrays nativas de python para uma string compreensivel pelo db
        """

        

        start_time=time.time()
  
        try:
            # 1. Dropa coluna id
            self.gdf_broken_glass.drop(columns='id', inplace=True)       

            # 2. Drop onde é id_layer = ['GRID'] através da condiução hexadecimal = 0. O objetivo é descartar geometrias que não tem nenhuma informação
            self.gdf_broken_glass=self.gdf_broken_glass[self.gdf_broken_glass['hexadecimal'] != 0]      

            # 3. Adiciona colunas cd_mun e cd_uf
            #Define a mask, que é onde existe 'MUN' na array
            mask = self.gdf_broken_glass['id_layer'].apply(lambda xs: 'MUN' in xs)

            self.gdf_broken_glass['cd_mun']=pd.NA
            self.gdf_broken_glass['cd_uf']=pd.NA

            #cd_mun apenas onde há 'MUN'. Isso evita warnings desnecessários.
            self.gdf_broken_glass.loc[mask, 'cd_mun'] = self.gdf_broken_glass.loc[mask].apply(
                lambda row: int(row['id_feature'][next(i for i, v in enumerate(row['id_layer']) if str(v).upper() == 'MUN')]),
                axis=1
            ).astype('int')

            #cd_uf apenas onde há 'MUN'. Isso evita warnings desnecessários.
            self.gdf_broken_glass.loc[mask,'cd_uf'] = (
                self.gdf_broken_glass.loc[mask, 'cd_mun']          
                .astype(str).str[:2]
                .astype('int')
            ).where(mask, other=pd.NA).astype('int')

            # 4. Conta número de CARs na feição
            self.gdf_broken_glass['n_car'] = np.array([x.count('CAR') for x in self.gdf_broken_glass['id_layer']])

            # 5. Adiciona colunas boleanas
            for coluna in self.boleanas:
                alvo = coluna.upper()
                self.gdf_broken_glass[f'is_{coluna.lower()}'] = \
                    self.gdf_broken_glass['id_layer'].apply(lambda xs: alvo in xs)   

            # 6. Calcula área das feicoes. Para isso é necessario descobrir a zona do grid e reprojar e dado de acordo com a feicao
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

            # 7. Cria a coluna id_layer_unico 
            self.gdf_broken_glass['id_layer_unico'] = self.gdf_broken_glass['id_layer'].apply(lambda x: np.unique(np.sort(x)))

            # 8. Converte as arrays nativas de python para uma string compreensivel pelo db
            self.gdf_broken_glass['id_layer'] = self.gdf_broken_glass['id_layer'].apply(lambda x: '{' + ','.join(map(str, x)) + '}')
            self.gdf_broken_glass['id_layer_unico'] = self.gdf_broken_glass['id_layer_unico'].apply(lambda x: '{' + ','.join(map(str, x)) + '}')
            self.gdf_broken_glass['id_feature'] = self.gdf_broken_glass['id_feature'].apply(lambda x: '{' + ','.join(map(str, x)) + '}')

            # 9. Faz o join com a tabela de categorias fundiárias
            tabela_indice=pd.read_csv(self.path_visao_fundiaria, sep = ';')            
            self.gdf_broken_glass=self.gdf_broken_glass.merge(tabela_indice, on=['hexadecimal',f'{self.coluna_hexadecimal}'], how = 'left')



        except Exception as e:
            logging.error(f"Erro observado {e}")
            raise


        
        #Libera memoria
        del gdf_proj
        del self.unidade_split


            

        elapsed_time=time.time()-start_time
        return f'{elapsed_time:.2f}'

    #Upload no db
    def upload_db(self, engine):
        """Funcao que fará o upload da tabela no banco"""

        memory = psutil.virtual_memory()
        cpu_percent = psutil.cpu_percent(interval=0.1)  
        start_time = time.time() 

        try:    
            self.gdf_broken_glass.to_postgis(
                    name=self.output_nome,
                    con=engine,
                    schema=self.output_schema,
                    if_exists="append",
                    index=False
                )
            logging.info(f"Iteração do grid {self.n_grid} armazenada - Uso de memória : {memory.percent}% - CPU : {cpu_percent}%")
        except Exception as e:
            logging.error(f"Falha no upload_db em {self.n_grid}, erro {e}")
            

        
        del self.gdf_broken_glass
        elapsed_time=time.time()-start_time
        return f'{elapsed_time:.2f}'

    #Run para 1 grid
    def run(self, n_grid):
        # Função que processa cada grid específico
        
        start_time=time.time()
        try:

            # Criar o engine dentro de cada processo
            engine = create_engine(
                f"postgresql://{self.db_user}:{self.db_password}@{self.db_host}:{self.db_port}/{self.db_name}"
            )

            
            intersection_time=self._intersection_sql(n_grid=n_grid, engine=engine)

            prepare_lines_time=self.prepare_split_line()

            perform_split_time=self.perform_split()

            overlapping_time=self.process_overlapping()                       

            format_gdf=self.format_gdf_broken_glass()

            #Inserir isso como método na classe
            upload_time=self.upload_db(engine=engine)

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
            
            logging.error(f"Iteração do grid {self.n_grid} ERRO {e}")
            
    #Paraleliza para uma lista de grids
    def run_parallel(self, grids):
        # Função para execução paralela
        with Pool(processes=self.num_processes) as pool:
            pool.map(self.run, grids)

        #feicoes descartadas
        # print('A')
        # print(self.feicoes_descartadas)
        # gdf=gpd.GeoDataFrame(data=self.feicoes_descartadas, geometry='geom',crs='EPSG:4674')
        # print(gdf)
        # gdf = gdf.set_geometry('geom')
        # self.feicoes_descartadas.to_file(f"finais/feicoes_descartadas_{self.output_nome}.shp")



# Uso da classe Splitter com logging

# if __name__ == "__main__":


#     # Carregar variáveis do .env para conexão com o banco
#     load_dotenv()
#     db_user = os.getenv("DB_USER")
#     db_password = os.getenv("DB_PASSWORD")
#     db_host = os.getenv("DB_HOST")
#     db_port = os.getenv("DB_PORT")
#     db_name = os.getenv("DB_NAME")


#     engine = create_engine(
#             f"postgresql://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}"
#         )

#     start_time = time.time()

#     with open("config.json", "r") as f:
#         config = json.load(f)  

   
#     splitter = Splitter()

#     splitter._intersection_sql(n_grid=13, engine=engine) # Foi criada a _intersection pois a antiga fazia apenas o touches, o que sobrecarregava a memoria
#     # splitter.intersection(13, data)
#     # splitter.prepare_split_line()
#     # splitter.perform_split()
#     # splitter.calculate_overlapping()
#     # splitter.save_results()


 
