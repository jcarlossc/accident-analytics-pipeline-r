# ==========================================================
# Script: data_validation.R
# Projeto: Sistema de Ingestão, Padronização, Limpeza e 
# Validação de Dados de Acidentes - Recife - 2019
# Autor: Carlos da Costa
# Localização: Recife, Pernambuco - Brasil
# Data de criação: 06/03/2026
# Última modificação: 06/03/2026
# Versão: 1.0.0
# Ambiente: development
#
# ---------------------------------------------------------
# DESCRIÇÃO:
# ---------------------------------------------------------
# Este script contém funções responsáveis pela validação
# estrutural e semântica do dataset de acidentes após a
# etapa de limpeza de dados (data_clean).
#
# A validação garante que os dados estejam consistentes
# antes de serem utilizados em análises, visualizações ou
# carregamento em banco de dados.
#
# ---------------------------------------------------------
# VALIDAÇÕES IMPLEMENTADAS:
# ---------------------------------------------------------
#
# 1. Estrutura do dataset
#    - Verifica se colunas possuem o tipo correto
#    - Data deve ser classe Date
#    - Hora deve ser classe hms
#
# 2. Validação de colunas numéricas
#    - Veículos e vítimas devem ser numéricos
#    - Não podem conter valores negativos
#
# 3. Consistência temporal
#    - Hora deve estar no intervalo válido
#    - Datas futuras não são permitidas
#
# 4. Qualidade dos dados
#    - Verificação de percentual de valores NA
#    - Alerta caso colunas tenham mais de 40% de valores ausentes
#
# Entrada:
#   dados -> data.frame ou tibble contendo os dados já limpos
#
# Saída:
#   TRUE se a validação for bem sucedida
#   erro caso alguma regra crítica seja violada
#
# ---------------------------------------------------------
# DEPENDÊNCIAS:
# ---------------------------------------------------------
#   logger
#   hms
#
# ---------------------------------------------------------
# OBSERVAÇÕES:
# ---------------------------------------------------------
#   Esta etapa deve ser executada após data_clean.R
#   e antes da etapa de análise ou armazenamento.
#
# ==========================================================

# --------------------------------------------------------
# 1. Pacotes utilizados
# --------------------------------------------------------
library(logger)
library(hms)

# ------------------------------------------------------
# 2. Função responsável pela validação dos dados
# ------------------------------------------------------
validate_data <- function(valid_data) {
  
  log_info("Iniciando validação dos dados")
  
  tryCatch({
    
    # -----------------------------------
    # 3. Verificar classes
    # -----------------------------------
    if (!inherits(valid_data$data, "Date"))
      stop("Coluna 'data' não é Date")
    
    if (!inherits(valid_data$hora, "hms"))
      stop("Coluna 'hora' não é hms")

    # -----------------------------------
    # 4. Verificar classes
    # -----------------------------------
    numeric_column <- c(
      "auto",
      "moto",
      "ciclom",
      "ciclista",
      "pedestre",
      "onibus",
      "caminhao",
      "viatura",
      "outros",
      "vitimas",
      "vitimasfatais"
    )
    
    invalid_columns <- numeric_column[
      !sapply(valid_data[numeric_column], is.numeric)
    ]
    
    if (length(invalid_columns) > 0) {
      stop(paste(
        "Colunas não numéricas:",
        paste(invalid_columns, collapse = ", ")
      ))
    }
    
    # -----------------------------------
    # 5. Verificar valores negativos
    # -----------------------------------
    if (any(valid_data[numeric_column] < 0, na.rm = TRUE))
      stop("Existem vítimas negativas")
    
    # -----------------------------------
    # 6. Intervalo de hora
    # -----------------------------------
    if (any(valid_data$hora > hms::as_hms("23:59:59"), na.rm = TRUE))
      stop("Hora inválida detectada")
    
    # -----------------------------------
    # 7. Datas futuras
    # -----------------------------------
    if (any(valid_data$data > Sys.Date(), na.rm = TRUE))
      stop("Data futura detectada")
    
    # -----------------------------------
    # 8. Percentual de NA
    # -----------------------------------
    percentual_na <- sapply(valid_data, function(x)
      mean(is.na(x)) * 100
    )
    
    if (any(percentual_na > 40))
      warning("Coluna com mais de 40% de NA")
    else(
      log_info("DataSet sem valores NA")
    )
    
    log_info("Validação concluída com sucesso")
    
    return(TRUE)
    
  # ----------------------------------------------------------
  # 9. Tratamento de ERRO
  # ----------------------------------------------------------
  }, error = function(e) {
    log_error("Erro na validação")
    log_error(e$message)
    stop(e)
  })
}