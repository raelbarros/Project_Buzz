## Desafio
> Migrar tabela de tokens de transação inteligente para um banco Postgre local com a periodicidade de D-1

### Atividades
- Subir um Airflow utilizando helm em um cluster kubernetes local; :x:
- Subir um banco Postgres local no mesmo cluster kubernetes para receber os dados; :heavy_check_mark:
- Construir uma DAG no Airflow capaz de integrar-se com o BigQuery e com o Postgres para migração diária; :heavy_check_mark:
- Versionar todo o código dos deployments do cluster kubernetes local (arquivos yaml) e do Airflow em um repositório git; :heavy_check_mark:
- Executar a DAG manualmente e verificar os dados chegando no banco Postgres; :heavy_check_mark:
- Executar um backfill de 7 dias para popular um histórico dos dados e verificar os dados chegando no banco Postgres. :heavy_check_mark:

:warning: Será feito uma explicação de como foi feito as tasks mais adiante;

### Estrutura de pastas
```
    .
    ├── dags                      # Dag do Airflow
    │     ├── configs             # Arquivos de Config (chave GCP e apontamento do banco)
    │     └── handler             # Funções de conexao com o BQ e o Postgre
    ├── database                  # Configurações do database
    │     ├── data                # Volume do database 
    │     └── init                # Scripts de execução com o start do db
    ├── logs                      # Logs do Airflow
    ├── plugins                   # Plugins do Airflow
    ├── docker-compose.yaml       # Arquivo de Deploy do ambiente
    ├── Dockerfile                # Arquivo de criação de image customizada do Airflow
    ├── requirements.txt          # Pacotes utilizados
    └── README.md
```

### Instalação
Ter em uma chave de autenticação na GCP para acesso ao Bigquery, a chave utilizada nesse projeto foi excluída por medida de segurança. A chave deve ser armazenada na pasta dags/configs/gcp_creds.json (caso salve com um nome diferente alterar o apontamento nos arquivos de DAG)

Criar uma imagem customizada do Airflow 
```
 $ docker build -t custom_airflow .
```
Executar o docker-compose
```
 $ docker-compose up -d
```

### Construção
Devido à falta de recurso na minha maquina local, não foi possível a realização da primeira atividade tentei utilizar o kind para deploy de um cluster de k8s, porém não tive sucesso devido a quantidade de memoria RAM da minha máquina pessoal.
Como alternativa foi utilizado um cluster do Airflow em cima apenas do Docker utilizando o docker-compose.
Para a criação do ambiente, foi utilizado uma imagem customizada com base o airflow:2.2.4,
Dentro do arquivo docker-compose.yaml foi acrescentado o serviço do Postgre relacionado a tarefa 2

```
database:
    image: postgres
    container_name: database
    hostname: database
    restart: always
    ports:
        - "54320:5432"
    deploy:
      resources:
        limits:
          memory: 500m
    volumes:
        - ./database/data:/var/lib/postgresql/data
        - ./database/init/create_tables.sql:/docker-entrypoint-initdb.d/create_tables.sql
    environment:
        POSTGRES_USER: root
        POSTGRES_PASSWORD: secret
```

Foram criadas duas DAGS:
1.	DAG_BQ_TO_PSQL_FIRST_MOVE: responsável pelo “first move” para popular a tabela com dados históricos;
2.	DAG_BQ_TO_PSQL: responsável pelo move D-1 que será executado diariamente.

Para a construção das DAGs foram criadas duas classes de conexão, uma para o BigQuery e outra para o Postgre (handlers), essas classes são chamadas a partir de um PythonOperator como função para fazer as devidas manipulações de dados.

### Validações
##### First_move:
A carga de first move foi realizado um select dos últimos 15-1 dias, para validação da carga pegamos o MAX(block_timestamp) do BigQuery e conferimos o valor do registro com o que foi inserido no db.
##### Carga Periódica:
A carga periódica é realizada com o um parâmetro de CURRENT_TIMESTAMP–1, com isso podemos verificar o registro em ambas bases novamente:


#### Documentações:
https://cloud.google.com/bigquery/docs/bigquery-storage-python-pandas
https://docs.sqlalchemy.org/en/14/core/engines.html
https://docs.sqlalchemy.org/en/14/dialects/postgresql.html#dialect-postgresql
https://airflow.apache.org/docs/apache-airflow/stable/start/docker.html

Afim de deixar registrado a minha tentativa de criação do cluster de k8s, foi utilizado as seguintes documentações:
https://airflow.apache.org/docs/helm-chart/stable/index.html
https://www.astronomer.io/events/recaps/official-airflow-helm-chart/
