# Pipeline de Dados Públicos

Pipeline de dados públicos usando Banco Central do Brasil, Airflow, ClickHouse, dbt, FastAPI e enriquecimento via Gemini.

O projeto extrai as séries SGS 432 (Selic) e 433 (IPCA), armazena os dados brutos no ClickHouse, transforma os dados com dbt, gera resumos em linguagem natural com Gemini e expõe os resultados por uma API REST.

## Arquitetura

```text
API SGS/BCB
    |
    v
Extractor Python ---> ClickHouse raw_indicadores
                              |
                              v
                        dbt staging/mart
                              |
                              v
                  Airflow task llm_enrich
                              |
                              v
                 ClickHouse indicadores_enriquecidos
                              |
                              v
                         FastAPI REST
```

Serviços executados pelo Docker Compose:

- `clickhouse`: armazenamento analítico.
- `postgres`: banco de metadados do Airflow.
- `airflow-scheduler`: execução e orquestração da DAG.
- `airflow-webserver`: interface web do Airflow.
- `api`: API FastAPI na porta `8000`.

## Pré-requisitos

- Docker Desktop ou Docker Engine com Docker Compose.
- Uma chave da API Gemini para executar o enriquecimento.

## Configuração

Copie o arquivo de exemplo:

```bash
cp .env.example .env
```

Edite o `.env` e configure pelo menos:

```env
LLM_PROVIDER=gemini
GEMINI_API_KEY=sua_chave_gemini
GEMINI_MODEL=gemini-3.8-flash
```

O arquivo `.env` real não deve ser commitado. O repositório contém somente o `.env.example` com valores fictícios.

## Execução

Suba toda a stack:

```bash
docker compose up -d --build
```

Confira os serviços:

```bash
docker compose ps -a
```

O `airflow-init` deve terminar com código `0`. ClickHouse e PostgreSQL devem aparecer como `healthy`.

## Airflow

Acesse a interface em:

```text
http://localhost:8080
```

Use o usuário `admin` e a senha definida em `AIRFLOW_ADMIN_PASSWORD` no `.env`.

A DAG principal é:

```text
pipeline_extracao_sgs_bcb
```

A sequência de tasks é:

```text
verify_clickhouse_connection
    -> run_pipeline
    -> run_dbt
    -> validate_data_load
    -> llm_enrich
```

Para verificar erros de importação:

```bash
docker compose exec airflow-scheduler airflow dags list-import-errors
```

Para executar a DAG pela CLI:

```bash
docker compose exec airflow-scheduler \
  airflow dags trigger pipeline_extracao_sgs_bcb
```

A task `llm_enrich` lê o mart mensal, envia um resumo dos indicadores ao Gemini e grava os resultados em:

```text
pipeline_dados.indicadores_enriquecidos
```

## API

Documentação Swagger:

```text
http://localhost:8000/docs
```

Health check:

```bash
curl http://localhost:8000/health
```

Listar indicadores:

```bash
curl "http://localhost:8000/indicadores?limit=2"
```

Consultar uma série:

```bash
curl http://localhost:8000/indicadores/433
```

Consultar a análise gerada pelo Gemini:

```bash
curl http://localhost:8000/indicadores/433/analise
```

Endpoints disponíveis:

- `GET /`
- `GET /health`
- `GET /indicadores`
- `GET /indicadores/{codigo_serie}`
- `GET /indicadores/{codigo_serie}/analise`

## Evidências da execução

Durante a validação foram obtidos:

- DAG completa com todas as tasks em estado `success`.
- Task `llm_enrich` concluída com sucesso.
- Resumos gerados pelo modelo `gemini-3.8-flash`.
- Endpoint `/indicadores/{codigo_serie}/analise` retornando os resumos persistidos.
- Swagger exibindo os endpoints da API.

### Airflow

Execução completa da DAG, com as cinco tasks concluídas:

![DAG completa com todas as tasks em sucesso](docs/screenshots/Screenshot%202026-09-10%20194813.png)

Detalhes da task `run_pipeline`:

![Detalhes da task run_pipeline](docs/screenshots/Screenshot%202026-09-10%20195000.png)

Detalhes da task `llm_enrich`:

![Detalhes da task llm_enrich](docs/screenshots/Screenshot%202026-09-10%20195707.png)

### FastAPI e Swagger

Health check retornando HTTP 200:

![Health check da API](docs/screenshots/Screenshot%202026-09-10%20195446.png)

Consulta dos indicadores mensais:

![Consulta de indicadores pela API](docs/screenshots/Screenshot%202026-09-10%20195837.png)

Consulta de uma série específica:

![Consulta da série 433](docs/screenshots/Screenshot%202026-09-10%20195742.png)

Análise gerada pelo Gemini para a série 433:

![Análise do indicador gerada pelo Gemini](docs/screenshots/Screenshot%202026-09-10%20195823.png)

As demais evidências da execução ficam disponíveis em [`docs/screenshots/`](docs/screenshots/).

## Desenvolvimento local

Para validar a sintaxe dos componentes Python:

```bash
.venv/bin/python -m py_compile \
  api/main.py \
  extractor/extract.py \
  extractor/enrich.py \
  airflow/dags/dag_sgs_extractor.py
```

Para executar somente o enriquecimento pela task do Airflow:

```bash
docker compose exec airflow-scheduler \
  airflow tasks test pipeline_extracao_sgs_bcb llm_enrich 2026-09-10
```

O enriquecimento pode ser desabilitado sem alterar a API principal:

```env
LLM_PROVIDER=none
```

Atualmente o adapter funcional de enriquecimento é o Gemini. As variáveis de Anthropic e OpenAI ficam preparadas para adapters futuros.

## Segurança

- Segredos são carregados por variáveis de ambiente.
- `.env` está no `.gitignore`.
- O `.dockerignore` impede o envio do `.env` e de artefatos locais para o build.
- O Dockerfile não contém chaves ou senhas.
- As consultas por código de série usam parâmetros do driver ClickHouse.

## Escopo e próximos passos

Este é um projeto demonstrativo para uma execução local. Não inclui autenticação de usuários, deploy em nuvem, Kubernetes, alta disponibilidade ou backups.

Próximas evoluções possíveis:

- adicionar adapters Anthropic e OpenAI;
- adicionar testes automatizados da API e do serviço LLM;
- adicionar paginação e filtros por período;
- executar a stack em ambiente de produção.
