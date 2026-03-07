# ======================================================================
# Arquivo: data_clean.R
# Projeto: Sistema de Ingestão, Padronização, Limpeza e 
# Validação de Dados de Acidentes - Recife - 2019
# Autor: Carlos da Costa
# Localização: Recife, Pernambuco - Brasil
# Data de criação: 06/03/2026
# Última modificação: 06/03/2026
# Versão: 1.0.0
# Ambiente: development
#
# ----------------------------------------------------------------------
# DESCRIÇÃO
# ----------------------------------------------------------------------
# Módulo responsável pela limpeza global (data cleaning) dos dados
# após a etapa de padronização estrutural.
#
# A função `clean_data()` executa procedimentos gerais de qualidade
# de dados, incluindo:
#
#   - Identificação e substituição de valores inválidos
#   - Normalização de valores ausentes (NA)
#   - Aplicação de regras de tratamento por tipo de variável
#   - Conversão adicional de datas e horários
#   - Remoção de registros inconsistentes
#   - Registro de métricas de qualidade no sistema de logs
#
# ----------------------------------------------------------------------
# ESCOPO
# ----------------------------------------------------------------------
# Este módulo tem como objetivo melhorar a qualidade dos dados
# antes da etapa de validação ou transformação analítica.
#
# Ele NÃO deve:
#
#   - Aplicar regras de negócio
#   - Criar agregações
#   - Realizar modelagem estatística
#   - Alterar a estrutura principal do dataset
#
# A responsabilidade deste módulo é tratar inconsistências
# comuns presentes em dados brutos ou semiestruturados.
#
# ----------------------------------------------------------------------
# PRINCIPAIS REGRAS DE LIMPEZA
# ----------------------------------------------------------------------
# - Conversão de valores inválidos para seus respectivos tipos
# - Normalização de textos vazios ou ignorados
# - Substituição de NA em variáveis numéricas quando aplicável
# - Conversão segura de datas e horários
# - Remoção de registros com hora inválida
#
# ----------------------------------------------------------------------
# DEPENDÊNCIAS
# ----------------------------------------------------------------------
# - dplyr
# - stringr
# - lubridate
# - hms
# - logger
#
# ----------------------------------------------------------------------
# TRATAMENTO DE ERROS
# ----------------------------------------------------------------------
# - Falhas estruturais interrompem o pipeline
# - Eventos são registrados via logger
# - tryCatch garante rastreabilidade de erros
# ======================================================================

# --------------------------------------------------------
# 1. Pacotes utilizados
# --------------------------------------------------------
library(dplyr)
library(stringr)
library(lubridate)
library(hms)
library(logger)

# ------------------------------------------------------
# 2. Função responsável pela limpeza dos dados
# ------------------------------------------------------
clean_data <- function(dados) {
  
  log_info("Iniciando limpeza global")
  
  tryCatch({
    
    # ------------------------------------------------------
    # 2. Validação
    # ------------------------------------------------------
    if (is.null(dados))
      stop("Objeto NULL")
    
    if (!is.data.frame(dados))
      stop("Objeto não é data.frame/tibble")
    
    if (nrow(dados) == 0)
      stop("Data frame vazio")
    
    log_info(paste("Linhas recebidas:", nrow(dados)))
    log_info(paste("Número de colunas:", ncol(dados)))
    
    # ------------------------------------------------------
    # 3. Padronizar valores inválidos
    # ------------------------------------------------------
    valores_invalidos <- c(
      "", " ", "NA", "N/A", "NULL",
      "NAO INFORMADO", "IGNORADO", "SEM INFORMACAO"
    )
    
    dados <- dados |>
      mutate(
        across(where(is.character), \(x) {
          x <- str_trim(x)
          x[x %in% valores_invalidos] <- NA
          x
        })
      )
    
    log_info("Valores inválidos convertidos para NA")
    
    # ------------------------------------------------------
    # 4. Regras por tipo
    # ------------------------------------------------------
    
    # Character 
    dados <- dados |>
      mutate(
        across(where(is.character), \(x)
               ifelse(is.na(x), "Não informado", x)
        )
      )
    
    # Numeric  por 0 (se regra permitir)
    dados <- dados |>
      mutate(
        across(where(is.numeric), \(x)
               replace(x, is.na(x), 0)
        )
      )
    
    # ------------------------------------------------------
    # 5. Regras por tipo
    # ------------------------------------------------------
    linhas_removidas <- sum(is.na(dados$hora))
    
    dados <- dados |> filter(!is.na(hora))
    
    message("Linhas removidas por hora inválida: ", linhas_removidas)
    
    # ------------------------------------------------------
    # 6. Log de NA restantes
    # ------------------------------------------------------
    na_count <- sapply(dados, \(x) sum(is.na(x)))
    log_info("NA por coluna após limpeza:")
    log_info(paste(names(na_count), na_count, collapse = " | "))
    
    log_info("Limpeza concluída")
    
    return(dados)
    
  }, error = function(e) {
    log_error("Erro na limpeza global")
    log_error(e$message)
    stop(e)
  })
}