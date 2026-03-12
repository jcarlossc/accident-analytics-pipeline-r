# 📌 Sistema de Ingestão, Padronização, Limpeza e Validação de Dados de Acidentes de Trânsito

Pipeline de engenharia de dados desenvolvido em R para ingestão, padronização, limpeza e validação de dados de acidentes de trânsito da cidade de Recife (2019).

## 📌 O projeto segue boas práticas de engenharia de dados, incluindo:
* Arquitetura modular
* Logging estruturado
* Tratamento de erros
* Retry automático
* Configuração por YAML
* Código reutilizável
* Este pipeline permite transformar dados brutos em dados confiáveis e prontos para análise.

## 📌 Objetivos do Projeto
Este projeto foi desenvolvido para:
* Demonstrar boas práticas de engenharia de dados em R
* Criar um pipeline robusto de preparação de dados
* Garantir qualidade e consistência dos dados
* Produzir logs auditáveis
* Facilitar manutenção e escalabilidade do código

## 📌 Pacotes Utilizados
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
## 📌 Configuração do Projeto (YAML)
Arquivos:
```
config/config.yaml
config/logging.yaml
config/paths.yaml
```
Examplo:
```
data:
  raw: "data/data_raw/acidentes_recife_2019.csv"
  processed: "data/data_processed/acidentes_recife_2019_processados.csv"

src:
  ingest: "src/ingestion/data_ingest.R"
  standardization: "src/standardization/data_standardization.R"
  clean: "src/cleaning/data_clean.R"
  validation: "src/validation/data_validation.R"
  
config:
  logger: "utils/logger/logger.R"
  handler: "utils/error/error_handler.R"
  helper: "utils/helpers/helpers.R"
  
logs:
  file: "logs/pipeline.log"
```
## 📌 Documentação das Colunas (alguns exemplos)
| Coluna |	Tipo | Descrição |
|--------|------|-----------|
| data |	Date | Data do acidente |
| hora |	Time | Hora do acidente |
| bairro |	Character | Bairro da ocorrência |
| auto |	Integer |	Número de automóveis |
| moto |	Integer |	Número de motocicletas |
| ciclista | Integer | Número de ciclistas |
| pedestre | Integer | Número de pedestres |
| vitimas | Integer |	Total de vítimas |
| vitimasfatais | Integer | Número de vítimas fatais |

## 📌 Métricas de Qualidade de Dados
Durante a validação, o pipeline verifica métricas importantes de qualidade.

| Métrica | Descrição |
| ------- | --------- |
| Completeness | Percentual de valores não nulos |
| Consistency | Consistência entre colunas |
| Validity	| Valores dentro do domínio permitido |
| Uniqueness | Verificação de duplicatas |
| Timeliness | Datas válidas |

Exemplo de validações:
* datas válidas
* horas válidas
* valores numéricos ≥ 0
* colunas obrigatórias presentes

## 📌 Sistema de Logging
O pipeline utiliza logger para registrar eventos.

Exemplo:
```
INFO  | Iniciando pipeline
INFO  | Ingestão concluída
```
Logs armazenados em:
```
logs/pipeline.log
```

## 📌 Tecnologias Utilizadas
* Linguagem R [https://cran.r-project.org/bin/windows/base/](https://cran.r-project.org/bin/windows/base/)<br>
* Sobre a Linguagem R [https://www.r-project.org/](https://www.r-project.org/)<br>
* Instalação da Linguagem R [https://informaticus77-r.blogspot.com/2025/09/blog-post.html](https://informaticus77-r.blogspot.com/2025/09/blog-post.html)<br>
* RStudio [https://posit.co/download/rstudio-desktop/](https://posit.co/download/rstudio-desktop/)<br>
* Microtutorial RStudio [https://informaticus77-r.blogspot.com/2025/09/blog-post_8.html](https://informaticus77-r.blogspot.com/2025/09/blog-post_8.html)<br>

## 📌 Modo de Utilizar
1. Baixar repositório do GitHub.
```
git clone https://github.com/jcarlossc/accident-analytics-pipeline-r.git
```
2. Entrar no diretório.
```
cd accident-analytics-pipeline-r
```
3. Restaurar ambiente.
```
renv::restore()
```
4. Executar o projeto no console do RStudio.
```
source("main.R")
```

## 📌 Gerenciamento de Ambiente (renv)
O projeto utiliza renv para garantir reprodutibilidade.
Instalar dependências
```
install.packages("renv")
```
Restaurar ambiente
```
renv::restore()
```

## 📌 Licença
Este projeto está licenciado sob MIT License.

## 📌 Contato
* Recife, PE - Brasil<br>
* Telefone: +55 81 99712 9140<br>
* Telegram: @jcarlossc<br>
* Pypi: https://pypi.org/user/jcarlossc/<br>
* Blogger linguagem R: [https://informaticus77-r.blogspot.com/](https://informaticus77-r.blogspot.com/)<br>
* Blogger linguagem Python: [https://informaticus77-python.blogspot.com/](https://informaticus77-python.blogspot.com/)<br>
* Email: jcarlossc1977@gmail.com<br>
* LinkedIn: https://www.linkedin.com/in/carlos-da-costa-669252149/<br>
* GitHub: https://github.com/jcarlossc<br>
* Kaggle: https://www.kaggle.com/jcarlossc/  
* Twitter/X: https://x.com/jcarlossc1977







