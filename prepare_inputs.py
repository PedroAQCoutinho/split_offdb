
import json
import os
from sqlalchemy import create_engine, text
from dotenv import load_dotenv
import time
import logging
from typing import Sequence, Union, Optional, List, Any


class DataProcessor:
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
            self.config = json.load(f)

        # Criar o engine de conexão
        self.engine = create_engine(
            f"postgresql://{self.db_user}:{self.db_password}@{self.db_host}:{self.db_port}/{self.db_name}"
        )


    def run_sql(
        self,
        sql: Union[str, Sequence[str]],
        fetch: bool = False,           # se True, retorna linhas de SELECTs
    ) -> Optional[List[Any]]:
        """
        Executa 1 ou N statements em transação única.
        - Se `fetch=True`, retorna lista de resultados (apenas dos statements que retornam linhas).
        - Em caso de erro, faz rollback automático e relança a exceção.
        """
        stmts: List[str] = [sql] if isinstance(sql, str) else list(sql)
        results: List[Any] = []

        # transação única; commit automático ao sair, rollback em exceção
        with self.engine.begin() as conn:
            for s in stmts:
                try:
                    res = conn.execute(text(s))
                    # Coleta resultados apenas quando houver linhas (SELECT)
                    if fetch and res.returns_rows:
                        rows = res.fetchall()
                        results.append(rows)
                    logging.info(f"Query executada com sucesso. \n {s}")
                    # NUNCA chame conn.commit() aqui
                except Exception as e:
                    # logger com stacktrace
                    logging.exception(f"Erro executando query: {s[:200]} ...")
                    raise  # deixa a exception subir (útil p/ quem chamou decidir)

        return results if fetch else None


    def check_table_exists(self, schema: str, tabela: str) -> bool:
        """Checa se a tabela existe em um schema e retorna True/False."""
        sql = f"""
            SELECT EXISTS (
                SELECT 1
                FROM information_schema.tables
                WHERE table_schema = '{schema}'
                AND table_name = '{tabela}'
            );
        """
        res = self.run_sql([sql], fetch=True)
        # se sua run_sql usar scalar(), isso já deve vir como [True] ou [False]
        return bool(res[0])



    def check_schema(self):
        """Verifica a existencia dos schemas necessários à criação do GRID e do INPUT"""
        # Verifica se existe
        
        for s in [self.config["grid"]["schema"], self.config["input_algoritmo_split"]["schema"], self.config["output"]["schema"]]:
    
            querie = f"""
                SELECT EXISTS (
                    SELECT 1
                    FROM information_schema.schemata
                    WHERE schema_name = '{s}'
                );
            """
            
            check = self.run_sql([querie], fetch=True)[0][0][0]


            if check:
                logging.info(f"Schema '{s}' já existe.")
            else:
                logging.info(f"Schema '{s}' não existe. Criando...")
                querie = f"CREATE SCHEMA {s};"
                self.run_sql([querie])
                logging.info(f"Schema '{s}' criado com sucesso.")




    def create_grid(self):
        """
        Cria um grid regular em cima da `tabela_base_grid` usando uma where_clause opcional.
        
        Requer PostGIS >= 3 (ST_SquareGrid).
        """
        grid_nome = f"{self.config["grid"]["schema"]}.{self.config["output"]["tabela_saida"]}"
        grid_nome = grid_nome + '_grid'
        
        # Construir a query de grid usando as cláusulas do config.json
        queries = []

        drop_grid = f'DROP TABLE IF EXISTS {grid_nome};'
        #Testa se é para sobrescrever
        if self.config["grid"]["overwrite"]:
            queries.append(drop_grid)

        grid_query = f"""
        CREATE TABLE {grid_nome} AS
        with feicoes as (SELECT (ST_SquareGrid(0.5, ST_MakeEnvelope(
        MIN(ST_XMin(geom)),
        MIN(ST_YMin(geom)),
        MAX(ST_XMax(geom)),
        MAX(ST_YMax(geom)),
        COALESCE(NULLIF(MAX(ST_SRID(geom)), 0), 4674)
        ))).geom AS geom
        FROM {self.config["grid"]["tabela_base"]} a {self.config["grid"]["where_clause"]})
        select row_number() over () id ,geom from feicoes
        where exists ( select true from {self.config["grid"]["tabela_base"]} a where st_intersects(a.geom, feicoes.geom));  
        """        

        queries.append(grid_query)
        #Cria tabela no banco
        try:
            self.run_sql(queries)
        except Exception as e:
            self.logger.error(f"A tabela ja existe, se quiser reescrevê-la mude para overwite = True \n Erro: {e}") 




    def create_input(self):
        """
        Cria a tabela input para o split com base no input.join. Este json conterá:
        - Nome da tabela  no banco (os dados tem que estar no banco)
        - SIGLA do dado (CAR, SIGEF, etc.)
        - Identificador único da feição (a pensar)
        - Hexadecimal conforme cartas da terra
        - geometria
        """
    

        #Index na coluna geom
        schema_in  = self.config['input_algoritmo_split']['schema']
        table_in   = self.config["output"]["tabela_saida"] + '_input'
        index_name = f"idx_{schema_in}_{table_in}_geom"



        #Querie para dropar tabela pré existente
        drop_querie = f'DROP TABLE IF EXISTS {schema_in}.{table_in};'
        #Querie para criar a tabela de input do modelo split schema do input + input_ + tabela_saida
        create_querie = f'CREATE TABLE {schema_in}.{table_in} AS '


        #Cria as queries individuais
        queries_individuais = []
        for input in self.config["tabelas_raw"]:

            q = f"""
            SELECT {input["id"]} id, '{input["id_layer"]}' id_layer, {input["hexadecimal"]} hexadecimal, {input["geom"]} geom 
            FROM {input["schema"]}.{input["nome_tabela"]} {input["where_clause"]} 
            """
            queries_individuais.append(q)


       
        #une todas as subqueries em uma só
        if len(queries_individuais) == 1:
            queries_individuais = queries_individuais[0]
        else:
            queries_individuais = " UNION ALL ".join(queries_individuais)
        
        

        queries = create_querie + queries_individuais + ';'


        logging.info(queries_individuais)
        

        index = f'CREATE INDEX IF NOT EXISTS {index_name} ' +  f'ON {schema_in}.{table_in} ' + f'USING GIST (geom);'
        
      

        #Se for para dar overwrite esse bloco será acionado
        if self.config['input_algoritmo_split']['overwrite'] and self.check_table_exists(schema = schema_in, tabela=table_in):
            queries = [drop_querie,queries,index]
        else:
            queries = []
            logging.info('Nenhuma querie foi adicionada pois overwrite = False')
    
        try:
            self.run_sql(queries)
            
        except Exception as e:

            logging.error(f"Deu pau {e}")
            

        





#TESTES
if __name__ == "__main__":

    # Configuração do logger para prepare_inputs.log. 
    logging.basicConfig(
        filename='logs/prepare_inputs.log',
        level=logging.INFO,
        filemode = 'w',
        format='%(asctime)s - %(levelname)s - %(message)s'
    )
    logger = logging.getLogger(__name__)

    # Define o tempo de início
    start_time = time.time()
    # Usa partial para fixar os primeiros três argumentos
    dataprocessor = DataProcessor()
    dataprocessor.check_schema()
    dataprocessor.create_grid() 
    dataprocessor.create_input()
   
    # Calcula o tempo decorrido
    elapsed_time = time.time() - start_time
    logging.info(f'Demorou {elapsed_time:.2f} segundos para rodar tudo')