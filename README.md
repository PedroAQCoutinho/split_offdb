#Split OFFDB

### Projeto de Divisão de Dados Geoespaciais Fora do Banco de Dados

`split_offdb` é uma ferramenta para realizar operações de divisão (split) em dados geoespaciais fora de um banco de dados, utilizando as bibliotecas `shapely`, `geopandas` e `multiprocessing`. 

## Motivação

A divisão de dados espaciais diretamente no banco de dados pode ser limitada por limitação do PostgreSQL. Este projeto fornece uma alternativa ao realizar essas operações fora do banco de dados, oferecendo escalabilidade e flexibilidade para processar grandes volumes de dados geoespaciais.

## Estrutura do Repositório

- **`main.py`**: Script principal que coordena a execução do projeto.
- **`split.py`**: Contém as funções específicas para realizar as operações de divisão nos dados geoespaciais.
- **`prepare_inputs.py`**: Prepara os dados de entrada, garantindo que estejam no formato correto para o processamento.
- **`config.json`**: Arquivo de configuração onde parâmetros e variáveis são definidos.
- **`requirements.txt`**: Lista das bibliotecas Python necessárias para rodar o projeto.
- **Diretórios adicionais**:
  - **`dev/`**: Scripts e notebooks para testes e desenvolvimento.
  - **`sql/`**: Scripts SQL (caso seja necessário manipular dados no banco antes do processamento externo).

## Instalação

1. **Clone o Repositório**:
   ```bash
   git clone https://github.com/PedroAQCoutinho/split_offdb.git
   cd split_offdb
   ```

2. **VENV e dependências**:
   Certifique-se de ter o Python 3.7+ instalado e execute:
   ```bash
   python -m venv _splitter
   source _splitter/bin/activate
   pip install -r requirements.txt
   ```

## Configuração

Antes de executar o projeto, edite o arquivo `config.json` para definir as configurações necessárias. Certifique-se de especificar corretamente os parâmetros de entrada, como diretórios de dados, configurações de saída e opções de divisão geoespacial.

Exemplo de configuração (`config.json`):
```json
{
    "grid_file": "./inputs/grid.parquet",
    "input_file": "./inputs/input.parquet",
    "arquivos_final":"split (Gerará um arquivo split.gpkg e um split.parquet)",
    "output_path": "./outputs/",
    "num_processes": 5,
    "grid_from_clause":"Querie que exporta o grid",
    "input_from_clause":"Querie que exporta o input (para inputs muito grandes cuidado, pois o dado é carrregado inteiro na memoria RAM) ",
    "grid_spacing": 0.5,
    "skip_input_gen":false,
    "skip_grid_gen":false,
    "skip_prepare_inputs":true

}
```

## Uso

Após configurar o `config.json`, execute o script principal para iniciar o processo de divisão de dados:

```bash
python main.py
```

O script usará as configurações definidas no `config.json` e processará os dados em paralelo em 'num_processes'.

## Funcionamento Interno

1. **Preparação dos Dados**: `prepare_inputs.py` exporta arquivos do banco de dados.
2. **Divisão dos Dados**: `split.py` executa a lógica de split.
3. **Processamento em Paralelo**: `multiprocessing` é utilizado para distribuir a carga de trabalho em várias CPUs, acelerando o processamento para grandes conjuntos de dados.


## Problemas

Atualmente o código tem um problema para lidar com grandes geometrias. Uma recomendação é quebrar as geometrias em pequenas geometrias


## Licença

Este projeto é distribuído sob a licença MIT. Consulte o arquivo `LICENSE` para mais detalhes.
