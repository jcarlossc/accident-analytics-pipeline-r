# ----------------------------------------------------------------------
# Arquivo: helpers.R
# Projeto: Sistema de Ingestão, Padronização, Limpeza e 
# Validação de Dados de Acidentes - Recife - 2019
# Autor: Carlos da Costa
# Localização: Recife, Pernambuco - Brasil
# Data de criação: 03/03/2026
# Última atualização: 11/04/2026
# Versão: 1.0.0
#
# ----------------------------------------------------------------------
# Descrição:
# Módulo de funções auxiliares (helpers) utilizadas em múltiplas partes
# do pipeline.
#
# Este script contém funções utilitárias responsáveis por:
#   - Validação de existência de arquivos
#   - Execução com retry manual
#   - Tratamento básico de erros operacionais
#
# As funções aqui definidas devem ser genéricas, reutilizáveis e
# independentes da lógica específica do pipeline.
#
# ----------------------------------------------------------------------
# Responsabilidades:
# - Evitar duplicação de código
# - Centralizar validações comuns
# - Oferecer mecanismos simples de tolerância a falhas
# - Mecanismo de logs e persistência de dados
#
# ----------------------------------------------------------------------
# Boas práticas:
# - Não incluir regras de negócio neste módulo.
# - Manter funções puras sempre que possível.
# - Evitar dependências externas desnecessárias.
#
# ----------------------------------------------------------------------
# Dependências:
# Nenhuma dependência obrigatória.
# (Pode integrar com logger ou error_handler se necessário.)
#
# ----------------------------------------------------------------------
# Integração:
# Este módulo pode ser utilizado por qualquer etapa do pipeline.
# 
# ----------------------------------------------------------------------

# ---------------------------------------------------------
# Função para teste de exixtência ou nulidade de arquivo
# ---------------------------------------------------------
check_file_exists <- function(path) {
  
  if (is.null(path) || !is.character(path)) {
    stop("Caminho inválido.")
  }
  
  if (!file.exists(path)) {
    stop(paste("Arquivo não encontrado:", path))
  }
  
  return(TRUE)
}

# --------------------------------------------------------
# Função Retry
# --------------------------------------------------------
retry_manual <- function(func, tentativas = 3, espera = 5) {
  
  for (i in 1:tentativas) {
    
    try_result <- try(func(), silent = TRUE)
    
    if (!inherits(try_result, "try-error")) {
      return(try_result)
    }
    
    Sys.sleep(espera)
  }
  
  stop("Todas as tentativas falharam.")
}

# --------------------------------------------------------
# Função wrapper para execução segura.
# --------------------------------------------------------
safe_run <- function(expr, etapa) {
  tryCatch(
    expr,
    error = function(e) {
      handle_error(e, etapa)
      stop(e)
    }
  )
}

# --------------------------------------------------------
# Função para log de início e fim de etapas
# --------------------------------------------------------
log_stage_start <- function(stage) {
  log_info(paste("Iniciando etapa:", stage))
}

log_stage_end <- function(stage) {
  log_info(paste("Finalizando etapa:", stage))
}

# --------------------------------------------------------
# Função para persistências dos dados
# --------------------------------------------------------
save_processed_data <- function(data, path) {
  
  log_info("Salvando dados processados")
  
  readr::write_csv(data, path)
  
  log_info("Dados salvos com sucesso")
}