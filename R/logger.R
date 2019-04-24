# logger setup

# !diagnostics suppress=


#' @export
log_is_active <- function() {
    
    identical(
        as.character(logger::log_layout())[1L],
        "logger::layout_glue_generator"
    )
}


#' @export
log_rotation <- function(dir_name = NULL, 
                         cfg = config_get()) {
    
    if (is.null(dir_name)) {
        path_pattern <- cfg$logger$path_pattern %||% "logs/log_h{hid}_w{wid}.log"
        stopifnot(is_key_chr(path_pattern))
        dir_name <- fs::path_dir(path_pattern)
    }
    if (!fs::dir_exists(dir_name)) fs::dir_create(dir_name)
    stopifnot(fs::dir_exists(dir_name))
    
    max_hist <- cfg$logger$max_hist %||% 9L
    stopifnot(is_key_int(max_hist))
    
    for (i in max_hist:1) {
        # delete old files
        hid_new <- formatC(i, width = 2, flag = "0")
        pattern <- glue::glue("{dir_name}/.*_h{hid_new}[_.]")
        del_path <- fs::dir_ls(dir_name, regexp = pattern, recursive = FALSE)
        if (length(del_path) > 0L) fs::file_delete(del_path)
        
        # rename files
        hid_old <- formatC(i - 1L, width = 2, flag = "0")
        pattern <- glue::glue("{dir_name}/.*_h{hid_old}[_.]")
        old_path <- fs::dir_ls(dir_name, regexp = pattern, recursive = FALSE)
        
        if (length(old_path) > 0L) {
            old <- glue::glue("(_h){hid_old}([_.])")
            new <- glue::glue("\\1{hid_new}\\2")
            new_path <- gsub(old, new, old_path)
            fs::file_move(old_path, new_path)
        }
    }
    
    invisible(NULL)
}


#' @export
log_config <- function(worker_id = 0L, 
                       cfg = config_get()) {
    
    level_chr <- cfg$logger$level %||% "INFO"
    if (cfg$debug && (level_chr != "TRACE")) level_chr <- "DEBUG"
    level <- utils::getFromNamespace(level_chr, "logger")
    
    wid <- formatC(worker_id, width = 2, flag = "0")
    log_layout_wid <- logger::layout_glue_generator(format = paste0(
        '{formatC(level, width = -7)} [w', wid, 
        '] [{format(time, "%Y-%d-%m %H:%M:%OS3")}] {msg}'))
    logger::log_layout(log_layout_wid)
    
    appender_index <- 1
    if ("console" %in% cfg$logger$appender) {
        logger::log_appender(
            logger::appender_console,
            index = appender_index
        )
        appender_index <- appender_index + 1
    }
    
    if ("file" %in% cfg$logger$appender) {
        path_pattern <- cfg$logger$path_pattern %||% "logs/log_h{hid}_w{wid}.log"
        stopifnot(is_key_chr(path_pattern))
        hid <- "00"
        file_path <- glue::glue(path_pattern)
        logger::log_appender(
            logger::appender_file(file_path), 
            index = appender_index
        )
    }
    
    logger::log_threshold(level)
}
