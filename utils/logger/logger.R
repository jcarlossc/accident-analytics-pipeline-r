# ======================================================================
# Arquivo: logger.R
# Projeto: Sistema de Ingestão, Padronização, Limpeza e 
# Validação de Dados de Acidentes - Recife - 2019
# Autor: Carlos da costa
# Localização: Recife, Pernambuco - Brasil
# Data de criação: 03/03/2026
# Última atualização: 03/03/2026
# Versão: 1.0.0
# Ambiente padrão: development
#
# ----------------------------------------------------------------------
# Descrição:
# Módulo responsável pela configuração centralizada do sistema de logs
# do pipeline.
#
# Este script define:
#   - Nível de log (INFO)
#   - Formato das mensagens
#   - Saída (console e/ou arquivo)
#   - Timezone e timestamp
#   - Política de sobrescrita de arquivos
#
# O comportamento do logger é configurado dinamicamente via config.yaml.
# ----------------------------------------------------------------------

# ------------------------------------------------------
# Pacotes utilizados
# ------------------------------------------------------
library(logger)
library(glue)
library(yaml)

# ------------------------------------------------------
# 2. Função responsável pela configuração do log
# ------------------------------------------------------
setup_logger <- function() {
  
  tryCatch({
    
    # --------------------------------------------------------
    # Ler arquivos de configuração
    # --------------------------------------------------------
    config_path <- suppressWarnings(
      yaml::read_yaml("config/paths.yaml")
    )
    
    config_logging <- suppressWarnings(
      yaml::read_yaml("config/logging.yaml")
    )  
    
    # --------------------------------------------------------
    # Validar campos obrigatórios
    # --------------------------------------------------------
    if (is.null(config_logging$logging$level)) {
      stop("Campo logging.level não encontrado em logging.yaml")
    }
    
    if (is.null(config_path$logs$file)) {
      stop("Campo logs.file não encontrado em paths.yaml")
    }
    
    # --------------------------------------------------------
    # Configurar logger
    # --------------------------------------------------------
    # Nível mínimo de log
    log_threshold(config_logging$logging$level)
    
    # Define formato da mensagem do log
    # Função do pacote logger que permite usar interpolação (glue)
    log_layout(layout_glue_generator(
      format = config_logging$format$format
    ))
    
    log_path <- config_paths$logs$file
    
    # Validação do config_paths$logs$file
    if (is.null(log_path) || !is.character(log_path) || length(log_path) != 1) {
      stop("logs$file inválido no YAML")
    }

    # Cria diretório, caso não exista
    log_dir <- dirname(log_path)
    
    if (!dir.exists(log_dir)) {
      dir.create(log_dir, recursive = TRUE)
    }
    
    # Define para onde o log será enviado.
    log_appender(appender_file(config_path$logs$file))
    
    # Registra mensagem de nível INFO
    log_info("Logger configurado com sucesso.")
    
  },
  
  # ----------------------------------------------------------
  # Tratamento de ERRO
  # ----------------------------------------------------------
  error = function(e) {
    
    # Como o logger pode não estar pronto ainda,
    # usamos message() como fallback
    message("ERRO ao configurar logger: ", e$message)
    
    # Opcional: tentar usar error_handler
    if (file.exists("utils/error_handler.R")) {
      source("utils/error_handler.R")
      handle_error(e, step = "SETUP_LOGGER")
    } else {
      stop(e$message)
    }
  },
  
  # ----------------------------------------------------------
  # Avisos
  # ----------------------------------------------------------
  warning = function(w) {
    message("Aviso no setup_logger: ", w$message)
  }
  
  )
  
}
