library(yaml)
library(logger)

# -------------------------------------------------
# 1. Carregar configuração
# -------------------------------------------------
config_paths <- suppressWarnings(
  yaml::read_yaml("config/paths.yaml")
)

# -------------------------------------------------
# 2. Carregar módulos
# -------------------------------------------------
source(config_paths$config$logger)
source(config_paths$config$handler)
source(config_paths$config$helper)
source(config_paths$src$ingest)
source(config_paths$src$standardization)
source(config_paths$src$clean)

# -------------------------------------------------
# 3. Inicializar logger
# -------------------------------------------------
setup_logger()

# -------------------------------------------------
# 4. Função principal do pipeline
# -------------------------------------------------
main <- function() {

  log_info("### Iniciando execução do pipeline - main.R ###")

  tryCatch({
  
    log_info("Iniciando ingentão de dados")
  
    tryCatch({
      # =======================================================
      # 1. SUB-PIPELINE (Ingestão dos dados)
      # =======================================================
    
      # --------------------------------------------------------
      # 5. Caminho completo do arquivo
      # --------------------------------------------------------  
      path_raw  <- config_paths$data$raw
    
      # --------------------------------------------------------
      # 6. Validar se arquivo existe
      # -------------------------------------------------------- 
      check_file_exists(path_raw)
      
      # --------------------------------------------------------
      # 7. 
      # -------------------------------------------------------- 
      data_ingest_tibble <- retry_manual(function() ingest_data(path_raw))
    
      log_info("Ingentão de dados finalizada")
      
    }, error = function(e) {
        handle_error(e, "Ingestão")
        stop(e)
    })
    
    tryCatch({
      # =======================================================
      # 2. SUB-PIPELINE (Padronização dos dados)
      # =======================================================
      
      log_info("Iniciando padronização de dados")
      
      # --------------------------------------------------------
      # 7. 
      # -------------------------------------------------------- 
      data_standard_tibble <- standardization_data(data_ingest_tibble)
      
      log_info("Padronização de dados finalizada")
      
    }, error = function(e) {
      handle_error(e, "Padronização")
      stop(e)
    })
    
    tryCatch({
      # =======================================================
      # 2. SUB-PIPELINE (Limpeza dos dados)
      # =======================================================
      
      log_info("Iniciando limpeza de dados")
      
      # --------------------------------------------------------
      # 7. 
      # -------------------------------------------------------- 
      data_clean_tibble <- clean_data(data_standard_tibble)
      
      log_info("Limpeza de dados finalizada")
      
    }, error = function(e) {
      handle_error(e, "Padronização")
      stop(e)
    })
    
    log_info("### Término da execução do pipeline - main.R ###")
  })

}


main()
