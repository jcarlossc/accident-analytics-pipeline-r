# ================================================================
# Arquivo: error_handler.R
# Projeto: Sistema de Ingestão, Padronização, Limpeza e 
# Validação de Dados de Acidentes - Recife - 2019
# Autor: Carlos da Costa
# Localização: Recife, Pernambuco - Brasil
# Data de criação: 03/03/2026
# Última modificação: 03/03/2026
# Versão: 1.0.0
# Ambiente: development
#
# Descrição:
# ----------------------------------------------------------------
# Módulo responsável pelo tratamento centralizado de erros do
# pipeline do sistema.
#
# Este script implementa funções para:
#   - Captura de exceções (tryCatch)
#   - Log estruturado de erros
#   - Retry automático (quando aplicável)
#   - Interrupção controlada do pipeline
#
# Dependências:
# ----------------------------------------------------------------
# - logger
# - glue
#
# Convenções:
# ----------------------------------------------------------------
# - Todos os erros críticos devem interromper o pipeline.
# - Erros não críticos devem ser registrados no log.
# - Nunca expor mensagens sensíveis no console em produção.
#
# Observações:
# ----------------------------------------------------------------
# Este módulo deve ser importado antes da execução do pipeline
# principal (main.R).
# ================================================================

# --------------------------------------------------------
# 1. Pacotes utilizados
# --------------------------------------------------------
library(logger)
library(glue)

handle_error <- function(e, step = "DESCONHECIDO") {
  
  tryCatch({
    # --------------------------------------------------------
    # 2. Monta mensagem estruturada
    # --------------------------------------------------------
    msg_step <- paste0("Erro na etapa: ", step)
    msg_error <- paste0("Mensagem original: ", e$message)
    
    # --------------------------------------------------------
    # 3. Tenta registrar no logger
    # --------------------------------------------------------
    log_error(msg_step)
    log_error(msg_error)
  },
  
  # ----------------------------------------------------------
  # 4. Se o logger falhar (ex: não inicializado)
  # ----------------------------------------------------------
  error = function(log_err) {
    message("Falha ao registrar no logger.")
    message("Detalhe logger: ", log_err$message)
    message("Erro original: ", e$message)
    
  },
  
  # ----------------------------------------------------------
  # 5. Sempre executa
  # ----------------------------------------------------------
  finally = {
    message("Execução será interrompida.")
  })
  
  
  # ----------------------------------------------------------
  # 6. Interrompe execução de forma controlada
  # ----------------------------------------------------------
  stop(paste0(
    "[PIPELINE_ERROR] Etapa: ", step,
    " | Mensagem: ", e$message
  ))
}
