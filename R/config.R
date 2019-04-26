# dispatcher for configurations found in config/config.yml

# !diagnostics suppress=


# see https://github.com/rstudio/config
# Add your hash to config.yml using `config_get_hash()`
# Remember to update the hash if the environment changes
#' @export
config_get_hash <- function() {
    
    digest::digest(Sys.info()[-(3:4)])
}


#' @export
config_get <- function() {
    
    cfg <- .PKGENV$CONFIG %||% list()
    
    invisible(cfg)
}


#' @export
config_set <- function(new_config) {
    
    stopifnot(rlang::is_bare_list(new_config))
    
    old_cfg <- config_get()
    updated_cfg <- purrr::list_modify(old_cfg, !!! new_config)
    assign("CONFIG", updated_cfg, .PKGENV)
    
    config_get()
}


#' @export
config_load <- function(config_active = NULL, 
                        config_file = "config/config.yml") {
    
    stopifnot(file.exists(config_file))
    
    # determine CONFIG_ACTIVE if missing
    if (!rlang::is_string(config_active)) {
        if (Sys.getenv("R_CONFIG_ACTIVE", "default") == "default") {
            # R_CONFIG_ACTIVE env var not set, use config_hash
            cfg_yml <- yaml::read_yaml(config_file, eval.expr = TRUE)
            hash <- config_get_hash()
            cfg_lgl <- vapply(
                cfg_yml, function(.x) {any(hash %in% .x[["config_hash"]])}, TRUE)
            config_active <- names(cfg_lgl[cfg_lgl])
            if (length(config_active) == 0L) {
                stop("No configuration found for hash `", hash, "`")
            } else if (length(config_active) >= 2L) {
                stop("Multiple configurations found for hash `", hash, "`")
            }
            cat("config_load: Using configuration `", config_active, 
                "` for hash `", hash, "`\n", sep = "")
        } else {
            config_active <- Sys.getenv("R_CONFIG_ACTIVE")
            cat("config_load: Using configuration `", config_active, 
                "` enforced by R_CONFIG_ACTIVE\n", sep = "")
        }
    }
    
    assign("CONFIG_ACTIVE", config_active, .PKGENV)
    assign("CONFIG_FILE", config_file, .PKGENV)
    new_config <- config::get(config = config_active, file = config_file)
    
    config_set(new_config)
}
