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

# -------------------------------------------------
# 3. Inicializar logger
# -------------------------------------------------
setup_logger()

# -------------------------------------------------
# 4. Função principal do pipeline
# -------------------------------------------------
#main <- function() {

  log_info("Iniciando execução do pipeline - main.R")

  tryCatch({
  
    log_info("Iniciando ingentão de dados")
  
    tryCatch({
    
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
    
    
    
    log_info("Término da execução do pipeline - main.R")
})

#}
data_ingest_tibble
class(data_ingest_tibble)

#main()
