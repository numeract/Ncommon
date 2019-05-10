# generic data stores

# !diagnostics suppress=.,


# helpers ----
ds_get_path <- function(ds, file_name = ds$data_file) {
    
    list(ds$connector$root, ds$connector$dir, file_name) %>%
        purrr::keep(is_key_chr) %>%
        do.call(file.path, .)
}


ds_set_connector <- function(ds, 
                             cfg = config_get()) {
    
    stopifnot(rlang::is_bare_list(ds))
    dc_name <- ds$connector
    stopifnot(is_key_chr(dc_name))
    
    connector_lst <- cfg$connector[[dc_name]]
    stopifnot(rlang::is_bare_list(connector_lst))
    
    dc_class <- connector_lst$class %||% dc_name
    stopifnot(is_key_chr(dc_class))
    
    if (dc_class %in% c("csv", "xls", "rds")) {
        # make connector config part of ds list
        ds$connector <- connector_lst
        # allow to overwrite connector root and dir
        if (is_key_chr(ds$root)) {
            ds$connector$root <- ds$root
            ds$root <- NULL
        }
        if (is_key_chr(ds$dir)) {
            ds$connector$dir <- ds$dir
            ds$dir <- NULL
        }
        # check only for dir existence now, file might not be available yet
        stopifnot(dir.exists(ds_get_path(ds, file_name = NULL)))
        # set classes
        classes <- c(paste0("ds_", dc_class), "ds_file")
        
    } else if (dc_class %in% c("mssql")) {
        # db connection must be available anytime
        db_connect(connector = ds$connector, cfg = cfg)
        # allow to overwrite connector schema, pk_pattern
        db_set_attr(
            connector = ds$connector, 
            schema = ds$schema %||% 
                db_get_attr("schema", connector = ds$connector),
            pk_pattern = ds$pk_pattern %||% 
                db_get_attr("pk_pattern", connector = ds$connector)
        )
        # set classes
        classes <- c(paste0("ds_", dc_class), "ds_sql")
        
    } else {
        rlang::abort(glue::glue("unrecognized connector {dc_class}"))
    }
    
    set_class(ds, classes)
}


ds_get_schema <- function(schema_file) {
    
    if (!is_key_chr(schema_file) || !file.exists(schema_file)) return(NULL)
    
    schema0 <- yaml::read_yaml(schema_file, eval.expr = TRUE)
    stopifnot(!is.null(schema0$`_default`))
    
    schema1 <- schema0
    schema1$`_default` <- NULL
    schema1 <-
        schema1 %>%
        purrr::map(.f = function(.x) {
            l <- purrr::list_modify(schema0$`_default`, !!!.x)
            l$r_default <- l$r_default %||% 
                do.call(paste0("as.", l$r_class), list(NA))
            l
        })
    
    schema1
}


ds_apply_schema <- function(ds, df, .check = TRUE) {
    
    stopifnot(!is.null(ds$schema))
    
    # keep all `always` cols and any present `optional` cols
    schema <- 
        ds$schema %>%
        purrr::keep(
            ~ isTRUE(.x$required) || identical(.x$required, "always") ||
            (identical(.x$required, "optional") && .x$name %in% colnames(df))
        )
    
    # process: new df with only output cols in schema order
    # allows new output cols to be created based on the same input col
    out_df <-
        schema %>%
        purrr::map_dfc(.f = function(.l) {
            # .x can be used in formula
            .x <- df[[.l$name]]
            if (!is.null(.l$r_process)) {
                frm <- stats::as.formula(.l$r_process)
                stopifnot(rlang::is_bare_formula(frm))
                y <- eval(rlang::f_rhs(frm))
            } else {
                y <- .x
            }
            
            y
        })
    
    # NA -> r_default, for each output col
    out0_df <- out_df
    schema %>%
        purrr::keep(~ !is.na(.x$r_default)) %>%
        purrr::iwalk(.f = function(.l, output_name) {
            out_df[[output_name]] <<- dplyr::coalesce(
                out0_df[[output_name]], .l$r_default)
        })
    
    if (.check) {
        # check classes
        col_class <- purrr::map_chr(schema, "r_class")
        out_class <- purrr::map_chr(out_df, get_class)
        stopifnot(identical(col_class, out_class))
    }
    
    out_df
}


# create ----

#' @export
ds_create <- function(ds_class, 
                      cfg = config_get()) {
    
    stopifnot(is_key_chr(ds_class))
    
    ds <- cfg$store[[ds_class]]
    ds <- ds_set_connector(ds = ds, cfg = cfg)
    ds$schema <- ds_get_schema(schema_file = ds$schema_file)
    
    prepend_class(ds, paste0("ds_", ds_class))
}


# generic ----

#' @export
ds_list_fields <- function(ds, ...) {
    UseMethod("ds_list_fields")
}

#' @export
ds_read_data <- function(ds, ...) {
    UseMethod("ds_read_data")
}

#' @export
ds_write_data <- function(ds, x, ...) {
    UseMethod("ds_write_data")
}

#' @export
ds_read <- function(ds, ...) {
    UseMethod("ds_read")
}

#' @export
ds_write <- function(ds, x, ...) {
    UseMethod("ds_write")
}

#' @export
ds_write_to <- function(x, ds, ...) {
    ds_write(ds, x, ...)
}


# default ----
ds_list_fields.default <- function(ds, ...) {
    rlang::abort("default abstract method `ds_list_fields`")
}


ds_read_data.default <- function(ds, ...) {
    rlang::abort("default abstract method `ds_read_data`")
}


ds_write_data.default <- function(ds, x, ...) {
    rlang::abort("default abstract method `ds_write_data`")
}


ds_read.default <- function(ds, ...) {
    rlang::abort("default abstract method `ds_read`")
}


ds_write.default <- function(ds, x, ...) {
    rlang::abort("default abstract method `ds_write`")
}


# file ----
ds_read.ds_file <- function(ds, ..., .check = TRUE) {
    
    stopifnot(!is.null(ds$schema))
    
    input_cols <- ds_list_fields(ds, ...)
    
    # required names must be present in input (maybe more than once)
    required_cols <- 
        ds$schema %>%
        purrr::keep(
            ~ isTRUE(.x$required) || identical(.x$required, "always")
        ) %>%
        purrr::map_chr("name")
    stopifnot(all(required_cols %in% input_cols))
    
    # message about new columns
    known_cols <- purrr::map_chr(ds$schema, "name")
    unknown_cols <- input_cols %if_not_in% known_cols
    if (length(unknown_cols) > 0L) {
        rlang::inform(
            paste("unknown fields:", paste0(unknown_cols, collapse = ", ")))
    }
    
    # get col_types for each input col
    col_types <- purrr::map_chr(ds$schema, "type")
    # only the first occurrence in `known_cols` is considered
    idx <- match(input_cols, known_cols)
    # sort by order in `input_cols``
    col_types <- col_types[idx]
    # skip unknown_cols
    col_types[is.na(col_types)] <- "skip"
    
    # read
    df <- ds_read_data(
        ds = ds,
        col_types = col_types,
        ...
    )
    
    ds_apply_schema(ds, df, .check = .check)
}


ds_write.ds_file <- function(ds, x, ...) {
    
    ds_write_data(ds, x, ...)
}


# csv ----
ds_list_fields.ds_csv <- function(ds, ...) {
    
    colnames(readr::read_csv(
        file = ds_get_path(ds),
        col_types = readr::cols(.default = readr::col_character()),
        n_max = 0
    ))
}


ds_read_data.ds_csv <- function(ds, col_types, ...) {
    
    dots <- list(...)
    # col_types to one string
    col_types[col_types %in% "skip"] <- "-"
    stopifnot(all(purrr::map_int(col_types, nchar) == 1L))
    dots$col_types <- paste0(col_types, collapse = "")
    
    arg <- c(list(file = ds_get_path(ds)), dots)
    df <- do.call(readr::read_csv, arg)
    
    attr(df, "spec") <- NULL
    
    df
}


ds_write_data.ds_csv <- function(ds, x, ...) {
    
    readr::write_csv(x, path = ds_get_path(ds), na = "NA")
}


# xls ----
ds_list_fields.ds_xls <- function(ds, ...) {
    
    dots <- list(...)
    # remove if still NULL
    dots$.name_repair <- dots$.name_repair %||% ds$connector$.name_repair
    dots$col_types <- "text"
    dots$n_max <- 0
    
    arg <- c(list(path = ds_get_path(ds)), dots)
    df <- do.call(readxl::read_excel, arg)
    
    colnames(df)
}


ds_read_data.ds_xls <- function(ds, col_types, ...) {
    
    dots <- list(...)
    # remove if still NULL
    dots$.name_repair <- dots$.name_repair %||% ds$connector$.name_repair
    dots$col_types <- col_types
    
    arg <- c(list(path = ds_get_path(ds)), dots)
    df <- do.call(readxl::read_excel, arg)
    
    df
}


ds_read.ds_xls <- function(ds, ..., .check = TRUE) {
    
    ds_read.ds_file(ds, sheet = ds$sheet, ..., .check = .check)
}



# rds ----
ds_list_fields.ds_rds <- function(ds, ...) {
    
    colnames(ds_read_data(ds))
}


ds_read_data.ds_rds <- function(ds, ...) {
    
    df <- readRDS(ds_get_path(ds))
    stopifnot(is.data.frame(df))
    
    df
}


ds_read.ds_rds <- function(ds, ..., .check = TRUE) {
    
    stopifnot(!is.null(ds$schema))
    
    # read once
    df <- ds_read_data(ds, ...)
    
    input_cols <- colnames(df)
    
    # required names must be present in input (maybe more than once)
    required_cols <- 
        ds$schema %>%
        purrr::keep(
            ~ isTRUE(.x$required) || identical(.x$required, "always")
        ) %>%
        purrr::map_chr("name")
    stopifnot(all(required_cols %in% input_cols))
    
    # message about new columns
    known_cols <- purrr::map_chr(ds$schema, "name")
    unknown_cols <- input_cols %if_not_in% known_cols
    if (length(unknown_cols) > 0L) {
        rlang::inform(
            paste("unknown fields:", paste0(unknown_cols, collapse = ", ")))
    }
    
    # already have col types, check for issues after processing
    ds_apply_schema(ds, df, .check = .check)
}


# mssql ----
ds_write_data.ds_mssql <- function(ds, x, ...) {
    
    db_insert(
        df = x,
        table = ds$table, 
        schema = ds$schema,
        connector = ds$connector
    )
}
