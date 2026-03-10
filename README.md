# 📌 Sistema de Ingestão, Padronização, Limpeza e Validação de Dados de Acidentes de Trânsito

Pipeline de engenharia de dados desenvolvido em R para ingestão, padronização, limpeza e validação de dados de acidentes de trânsito da cidade de Recife (2019).

## 📌 O projeto segue boas práticas de engenharia de dados, incluindo:
* Arquitetura modular
* Logging estruturado
* Tratamento de erros
* Retry automático
* Configuração por YAML
* Código reutilizável e testável
Este pipeline permite transformar dados brutos em dados confiáveis e prontos para análise.

## 📌 Objetivos do Projeto
* Este projeto foi desenvolvido para:
* Demonstrar boas práticas de engenharia de dados em R
* Criar um pipeline robusto de preparação de dados
* Garantir qualidade e consistência dos dados
* Produzir logs auditáveis
* Facilitar manutenção e escalabilidade do código

## 📌 Tecnologias Utilizadas
Principais bibliotecas utilizadas:
| Biblioteca |	Função |
|------------|---------|
| tidyverse |	Manipulação de dados |
| readr |	Leitura eficiente de arquivos |
| yaml |	Configuração do projeto |
| logger | Sistema de logs |
| retry | Reexecução automática em caso de falha |
| logger | Sistema de log |
| lubridate |	Manipulação de datas |

## 📌 Arquitetura do Pipeline
O pipeline segue um fluxo típico de ETL (Extract → Transform → Load).
```
           +------------------+
           |  Dados Brutos    |
           |  acidentes_2019  |
           +--------+---------+
                    v
           +------------------+
           |  Ingestão        |
           |  data_ingest.R   |
           +--------+---------+
                    v
           +---------------------------+
           | Padronização              |
           | data_standardization.R    |
           +--------+------------------+
                    v
           +------------------+
           | Limpeza          |
           | data_clean.R     |
           +--------+---------+
                    v
           +------------------+
           | Validação        |
           | data_validate.R  |
           +--------+---------+
                    v
           +------------------+
           | Dados Processados|
           +------------------+
```

## 📌 Estrutura do Projeto
```
accident-analytics-pipeline-r/
│
├── config/
│   ├── config.yaml
|   ├── logging.yaml
|   └── paths.yaml
|
├── data/
│   ├── data_raw/
│   │   └── acidentes_recife_2019.csv
│   └── data_processed/
|
├── logs/
│   └── pipeline.log
|
├── renv/
|
├── src/
│   ├── cleaning/
│   │   └── data_clean.R
│   ├── ingestion/
│   │   └── data_ingest.R
│   ├── standardization/
│   │   └── data_standardization.R
│   └── validation/
│       └── data_validation.R
|
├── utils/
│   ├── error/
│   │   └── error_handler.R
│   ├── helpers/
│   │   └── helpers.R
│   └── logger/
│       └── logger.R
|
├── main.R
├── accident-analytics-pipeline-r.Rproj
├── .Rprofine
├── .Rhistory
├── .RData
├── .gitignore
├── README.md
└── renv.lock
```
