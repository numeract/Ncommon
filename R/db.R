# db functions

# !diagnostics suppress=.


# connection string ----
db_get_connector <- function(connector = NULL,
                             cfg = config_get()) {
    
    connector <- connector %||% cfg$connector$db_default %||% NA_character_
    
    # not only a valid label, but the connector info list exits
    if (!is.list(cfg$connector[[connector]])) {
        rlang::abort(glue::glue("connector config not found: {connector}"))
    }
    
    connector
}


db_get_connector_info <- function(connector = NULL,
                                  cfg = config_get()) {
    
    connector <- db_get_connector(connector = connector, cfg = cfg)
    connector_lst <- cfg$connector[[connector]]
    
    # do not print secrets by default
    invisible(connector_lst)
}


db_make_connection_string <- function(dsn = NULL,
                                      driver = NULL,
                                      server = NULL,
                                      port = NULL,
                                      database = NULL,
                                      uid = NULL,
                                      pwd = NULL,
                                      connection_string = NULL,
                                      ...,
                                      connector = NULL,
                                      cfg = config_get()) {
    
    connector_lst <- db_get_connector_info(connector = connector, cfg = cfg)
    
    # follow odbc:::OdbcConnection()
    # args take precedence over `connector_lst`
    # from `connector_lst` pick only desired elements, otherwise NULL
    # connection_string, everything else is added to it
    args <- c(
        dsn = dsn %||% connector_lst$dsn,
        driver = driver %||% connector_lst$driver,
        server = server %||% connector_lst$server,
        port = port %||% connector_lst$port,
        database = database %||% connector_lst$database,
        uid = uid %||% connector_lst$uid,
        pwd = pwd %||% connector_lst$pwd,
        connection_string = 
            connection_string %||% connector_lst$connection_string,
        list(...)
    )
    stopifnot(all(rlang::have_name(args)))
    if (!is.null(connection_string)) stopifnot(grepl(";$", connection_string))
    
    connection_string <- paste0(
        connection_string, 
        paste(collapse = ";", sep = "=", names(args), args)
    )
    
    # do not print secrets by default
    invisible(connection_string)
}


# keyring ----
db_get_connection_string <- function(connector = NULL,
                                     cfg = config_get()) {
    
    connector_lst <- db_get_connector_info(connector = connector, cfg = cfg)
    connection_string <- NULL
    
    # try to get a connection_string from the keyring
    if (!is.null(connector_lst$keyring_service) && 
        !is.null(connector_lst$keyring_username)
    ) {
        tryCatch({
            connection_string <- keyring::key_get(
                service = connector_lst$keyring_service, 
                username = connector_lst$keyring_username, 
            )
            rlang::inform("Using connection string from keyring")
        }, error = function(e) {
            NULL
        })
    }
    
    # try to make a connection string from connector_lst
    if (!is_key_chr(connection_string)) {
        connection_string <- db_make_connection_string(
            connector = connector, cfg = cfg)
        rlang::inform("Using connection info from connector")
    }
    
    # do not print secrets by default
    invisible(connection_string)
}


db_set_connection_string <- function(connection_string = NULL,
                                     connector = NULL,
                                     cfg = config_get()) {
    
    connector_lst <- db_get_connector_info(connector = connector, cfg = cfg)
    
    # try to make a connection string from connector
    if (is.null(connection_string)) {
        connection_string <- db_make_connection_string(
            connector = connector, cfg = cfg)
    }
    if (!is_key_chr(connection_string)) {
        rlang::abort("connection_string is empty")
    }
    
    if (is.null(connector_lst$keyring_service) || 
        is.null(connector_lst$keyring_username)
    ) {
        rlang::abort("keyring fields not defined, cannot set keyring")
    }
    # fail if error
    keyring::key_set_with_value(
        service = connector_lst$keyring_service, 
        username = connector_lst$keyring_username, 
        password = connection_string
    )
    
    # do not print secrets by default
    invisible(connection_string)
}


db_del_connection_string <- function(connector = NULL,
                                     cfg = config_get()) {
    
    connector_lst <- db_get_connector_info(connector = connector, cfg = cfg)
    
    if (is.null(connector_lst$keyring_service) || 
        is.null(connector_lst$keyring_username)
    ) {
        rlang::warn("keyring fields not defined, nothing to delete")
    } else {
        tryCatch({
            keyring::key_delete(
                service = connector_lst$keyring_service, 
                username = connector_lst$keyring_username
            )
        }, error = function(e) {
            rlang::warn(e$message)
        })
    }
    
    invisible(NULL)
}


# pool ----
db_get_pool_lst <- function() {
    
    if (!exists("POOL", .PKGENV)) {
        assign("POOL", list(), .PKGENV)
    }
    
    if (is.null(.PKGENV$POOL$db_default)) {
        .PKGENV$POOL$db_default <- NA_character_
    }
    
    invisible(.PKGENV$POOL)
}


db_get_pool <- function(connector = NULL) {
    
    POOL <- db_get_pool_lst()
    
    connector <- connector %||% POOL$db_default %||% NA_character_
    
    # connector or pool might be null (not tested here)
    invisible(POOL[[connector]])
}


#' @export
db_is_valid <- function(connector = NULL) {
    
    pl <- db_get_pool(connector = connector)
    
    !is.null(pl) && pool::dbIsValid(pl)
}


# connection ----

#' @export
db_connect <- function(connector = NULL,
                       cfg = config_get()) {
    
    if (!db_is_valid(connector = connector)) {
        # validate connector
        connector <- db_get_connector(connector = connector, cfg = cfg)
        connector_lst <- db_get_connector_info(connector = connector, cfg = cfg)
        
        # checks
        stopifnot(is_key_chr(connector_lst$database))
        stopifnot(is_key_chr(connector_lst$schema))
        patterns <- connector_lst$patterns %||% list()
        patterns$pk <- patterns$pk %||% "{table}_key"
        patterns$key <- patterns$key %||% "_key$"
        patterns$id <- patterns$id %||% "_id$"
        patterns$bin <- patterns$bin %||% "_bin$"
        patterns$json <- patterns$json %||% "_json$"
        stopifnot(is_key_chr(patterns$pk))
        stopifnot(is_key_chr(patterns$key))
        stopifnot(is_key_chr(patterns$id))
        stopifnot(is_key_chr(patterns$bin))
        stopifnot(is_key_chr(patterns$json))
        
        # create pool
        pl <- pool::dbPool(
            drv = odbc::odbc(),
            .connection_string = db_get_connection_string(
                connector = connector, cfg = cfg)
        )
        .PKGENV$POOL[[connector]] <- pl
        
        # known attributes
        db_set_attr(
            database = connector_lst$database,
            schema = connector_lst$schema,
            patterns = patterns,
            connector = connector
        )
        
        # check whether to set the default connector
        if (identical(connector, cfg$connector$db_default)) {
            .PKGENV$POOL$db_default <- connector
        }
    }
    
    invisible(db_get_pool(connector = connector))
}


#' @export
db_close <- function(connector = NULL) {
    
    if (db_is_valid(connector = connector)) {
        # either connector is valid or NULL to get the default
        POOL <- db_get_pool_lst()
        connector <- connector %||% POOL$db_default
        pl <- db_get_pool(connector = connector)
        pool::poolClose(pl)
        .PKGENV$POOL[[connector]] <- NULL
        # check whether to reset the default connector
        if (identical(connector, .PKGENV$POOL$db_default)) {
            .PKGENV$POOL$db_default <- NA_character_
        }
    }
    
    invisible(NULL)
}


#' @export
db_close_all <- function() {
    
    POOL <- db_get_pool_lst()
    POOL$db_default <- NULL
    POOL %>%
        purrr::iwalk(.f = function(pl, connector) {
            pool::poolClose(pl)
            .PKGENV$POOL[[connector]] <- NULL
        })
    .PKGENV$POOL$db_default <- NA_character_
    
    invisible(NULL)
}


# attr ----
db_set_attr <- function(..., 
                        connector = NULL) {
    
    POOL <- db_get_pool_lst()
    connector <- connector %||% POOL$db_default %||% NA_character_
    stopifnot(db_is_valid(connector = connector))
    
    attr_lst <- list(...)
    stopifnot(all(rlang::have_name(attr_lst)))
    
    attr_lst %>%
        purrr::iwalk(.f = function(attr_value, attr_name){
            attr(.PKGENV$POOL[[connector]], attr_name) <- attr_value
        })
    
    invisible(NULL)
}


db_get_attr <- function(attr_name,
                        connector = NULL) {
    
    pl <- db_get_pool(connector = connector)
    
    attr(pl, attr_name)
}


#' @export
db_get_database <- function(connector = NULL) {
    
    db_get_attr("database", connector = connector)
}


#' @export
db_get_schema <- function(connector = NULL) {
    
    db_get_attr("schema", connector = connector)
}


#' @export
db_get_pattern <- function(pattern_name,
                           connector = NULL) {
    
    patterns <- db_get_attr("patterns", connector = connector)
    
    patterns[[pattern_name]]
}


#' @export
db_get_pattern_cols <- function(df, 
                                pattern_name,
                                cols = NULL,
                                connector = NULL) {
    
    if (isTRUE(is.na(cols))) return(character())
    
    if (is.null(cols)) {
        pattern <- db_get_pattern(pattern_name, connector = connector)
        stopifnot(is_key_chr(pattern))
        cols <- tidyselect::vars_select(
            names(df), tidyselect::matches(pattern))
    }
    
    msk <- rlang::has_name(df, cols)
    cols <- cols[msk]
    
    cols
}


# bin ----
to_bin <- function(x) {
    
    as.raw(serialize(x, NULL))
}


to_bin_lst <- function(x) {
    
    purrr::map(x, to_bin)
}


to_bin_df <- function(df, 
                      bin_cols = NULL,
                      connector = NULL) {
    
    bin_cols <- db_get_pattern_cols(
        df, pattern_name = "bin", cols = bin_cols, connector = connector)
    if (length(bin_cols) == 0L) return(df)
    
    dplyr::mutate_at(df, bin_cols, to_bin_lst)
}


from_bin <- function(x) {
    
    unserialize(as.raw(x))
}


from_bin_lst <- function(x) {
    
    purrr::map(x, from_bin)
}


from_bin_df <- function(df,
                        bin_cols = NULL,
                        connector = NULL) {
    
    bin_cols <- db_get_pattern_cols(
        df, pattern_name = "bin", cols = bin_cols, connector = connector)
    if (length(bin_cols) == 0L) return(df)
    
    dplyr::mutate_at(df, bin_cols, from_bin_lst)
}


# json ----
to_json <- function(x) {
    
    jsonlite::toJSON(x)
}


to_json_lst <- function(x) {
    
    purrr::map(x, to_json)
}


to_json_df <- function(df, 
                       json_cols = NULL,
                       connector = NULL) {
    
    json_cols <- db_get_pattern_cols(
        df, pattern_name = "json", cols = json_cols, connector = connector)
    if (length(json_cols) == 0L) return(df)
    
    dplyr::mutate_at(df, json_cols, to_json_lst)
}


from_json <- function(x) {
    
    jsonlite::fromJSON(x)
}


from_json_lst <- function(x) {
    
    purrr::map(x, from_json)
}


from_json_df <- function(df,
                         json_cols = NULL,
                         connector = NULL) {
    
    json_cols <- db_get_pattern_cols(
        df, pattern_name = "json", cols = json_cols, connector = connector)
    if (length(json_cols) == 0L) return(df)
    
    dplyr::mutate_at(df, json_cols, from_json_lst)
}


# tbl ----

#' @export
db_list_tbl <- function(table_type = NULL,
                        schema = NULL,
                        connector = NULL) {
    
    stopifnot(
        is.null(table_type) || table_type %in% c("TABLE", "VIEW", "TABLE,VIEW"))
    schema <- schema %||% db_get_schema(connector = connector)
    
    conn <- pool::poolCheckout(db_connect(connector = connector))
    on.exit(pool::poolReturn(conn))
    
    odbc::dbListTables(
        conn = conn, 
        schema_name = schema, 
        table_type = table_type
    )
}


#' @export
db_tbl_exists <- function(table, 
                          tables_only = FALSE,
                          schema = NULL,
                          connector = NULL) {
    
    stopifnot(is_key_chr(table))
    
    if (isTRUE(tables_only)) {
        table_type <- "TABLE"
    } else {
        table_type <- NULL
    }
    tbl_names <- db_list_tbl(
        table_type = table_type, schema = schema, connector = connector)
    
    table %in% tbl_names
}


#' @export
db_require_tbl <- function(table,
                           tables_only = FALSE,
                           schema = NULL,
                           connector = NULL) {
    
    stopifnot(db_tbl_exists(
        table = table, 
        tables_only = tables_only,
        schema = schema, 
        connector = connector)
    )
}


#' @export
db_create_tbl <- function(table, 
                          create_sql,
                          force_new = FALSE,
                          schema = NULL,
                          connector = NULL) {
    
    stopifnot(is_key_chr(create_sql))
    schema <- schema %||% db_get_schema(connector = connector)
    
    conn <- pool::poolCheckout(db_connect(connector = connector))
    on.exit(pool::poolReturn(conn))
    
    # remove table if force_new
    tbl_exists <- db_tbl_exists(
        table = table, tables_only = TRUE, schema = schema, connector = connector)
    if (isTRUE(force_new) && tbl_exists) {
        sql <- "DROP TABLE [{schema}].[{table}]"
        DBI::dbExecute(conn, sql)
    }
    
    # create table if it does not exists
    tbl_exists <- db_tbl_exists(
        table = table, tables_only = TRUE, schema = schema, connector = connector)
    if (!tbl_exists) {
        DBI::dbExecute(conn, create_sql)
    }
    
    db_tbl_exists(
        table = table, tables_only = TRUE, schema = schema, connector = connector)
}


#' @export
db_tbl <- function(table,
                   schema = NULL,
                   connector = NULL) {
    
    schema <- schema %||% db_get_schema(connector = connector)
    db_require_tbl(
        table = table, tables_only = FALSE, schema = schema, connector = connector)
    
    if (is.null(schema)) {
        dplyr::tbl(db_connect(connector = connector), table)
    } else {
        dplyr::tbl(
            db_connect(connector = connector), 
            dbplyr::in_schema(schema = schema, table = table)
        )
    }
}


#' @export
db_collect <- function(tbl,
                       bin_cols = NULL,
                       json_cols = NULL,
                       connector = NULL) {
    
    df <- dplyr::collect(tbl)
    df <- from_bin_df(df, bin_cols = bin_cols, connector = connector)
    df <- from_json_df(df, json_cols = json_cols, connector = connector)
    
    df
}


# field ----

#' @export
db_list_fields <- function(table,
                           schema = NULL,
                           connector = NULL) {
    
    schema <- schema %||% db_get_schema(connector = connector)
    
    conn <- pool::poolCheckout(db_connect(connector = connector))
    on.exit(pool::poolReturn(conn))
    
    odbc::dbListFields(conn = conn, name = table, schema_name = schema)
}


#' @export
db_get_pk <- function(table,
                      schema = NULL,
                      connector = NULL) {
    
    stopifnot(is_key_chr(table))
    
    schema <- schema %||% db_get_schema(connector = connector)
    pk_pattern <- db_get_pattern("pk", connector = connector)
    
    # default pattern uses var `table`
    as.character(glue::glue(pk_pattern))
}


#' @export
db_get_max <- function(field,
                       table,
                       schema = NULL,
                       connector = NULL) {
    
    val <- NULL
    
    stopifnot(is_key_chr(field))
    
    df <- 
        db_tbl(table = table, schema = schema, connector = connector) %>%
        dplyr::select(val = dplyr::one_of(field)) %>%
        dplyr::summarise(val = max(val, na.rm = TRUE)) %>%
        dplyr::collect()
    
    if (is.na(df$val)) {
        0L
    } else {
        df$val
    }
}


# basic ----

#' @export
db_insert <- function(df,
                      table, 
                      schema = NULL,
                      connector = NULL) {
    # follow odbc:::odbc_write_table but modify sql string
    
    schema <- schema %||% db_get_schema(connector = connector)
    db_require_tbl(
        table = table, tables_only = TRUE, schema = schema, connector = connector)
    if (nrow(df) == 0L) return(invisible(0L))
    
    conn <- pool::poolCheckout(db_connect(connector = connector))
    on.exit(pool::poolReturn(conn))
    
    # local def of odbc:: functions
    OdbcResult <- utils::getFromNamespace("OdbcResult", ns = "odbc")
    result_insert_dataframe <- utils::getFromNamespace(
        "result_insert_dataframe", ns = "odbc")
    
    # remove PK col from df, if present (do not insert it)
    pk_col <- db_get_pk(table = table, schema = schema, connector = connector)
    pk_col <- pk_col %if_in% colnames(df)
    df <- dplyr::select(df, -dplyr::one_of(pk_col))
    db_fields <- odbc::dbListFields(conn, name = table, schema_name = schema)
    df_fields <- names(df)
    stopifnot(all(df_fields %in% db_fields))
    
    # create SQL statement
    df_fields <- odbc::dbQuoteIdentifier(conn, df_fields)
    params <- rep("?", length(df_fields))
    sql <- as.character(glue::glue(
        "INSERT INTO [{schema}].[{table}] (",
        paste0(df_fields, collapse = ", "), ")\n",
        "VALUES (", paste0(params, collapse = ", "), ")"
    ))
    sql_values <- odbc::sqlData(conn, df)
    
    n <- 0L
    tryCatch({
        rs <- OdbcResult(conn, sql)
        result_insert_dataframe(rs@ptr, sql_values)
        n <- nrow(df)
    },
        finally = if (exists("rs")) odbc::dbClearResult(rs)
    )
    
    invisible(n)
}


#' @export
db_update <- function(df,
                      table, 
                      schema = NULL,
                      connector = NULL) {
    
    schema <- schema %||% db_get_schema(connector = connector)
    db_require_tbl(
        table = table, tables_only = TRUE, schema = schema, connector = connector)
    if (nrow(df) == 0L) return(invisible(0L))
    
    conn <- pool::poolCheckout(db_connect(connector = connector))
    on.exit(pool::poolReturn(conn))
    
    # PK field must be the first column & col names must match
    pk_col <- db_get_pk(table = table, schema = schema, connector = connector)
    db_fields <- odbc::dbListFields(conn, name = table, schema_name = schema)
    df_fields <- names(df)
    stopifnot(identical(df_fields[1L], pk_col))
    stopifnot(identical(db_fields[1L], pk_col))
    stopifnot(all(df_fields %in% db_fields))
    
    # create SQL statement
    df_fields <- odbc::dbQuoteIdentifier(conn, df_fields)
    sql <- as.character(glue::glue(
        "UPDATE [{schema}].[{table}]",
        " SET ", paste0(df_fields[-1L], "=?", collapse = ", "),
        " WHERE {df_fields[1L]}=?", 
    ))
    sql_values <- 
        odbc::sqlData(conn, df) %>%
        dplyr::select(-1L, dplyr::everything())
    
    n <- 0L
    tryCatch({
        rs <- odbc::dbSendQuery(conn, sql)
        odbc::dbBind(rs, sql_values)
        n <- odbc::dbGetRowsAffected(rs)
    },
        finally = if (exists("rs")) odbc::dbClearResult(rs)
    )
    
    invisible(n)
}


#' @export
db_query <- function(query_path,
                     connector = NULL) {
    
    conn <- pool::poolCheckout(db_connect(connector = connector))
    on.exit(pool::poolReturn(conn))
    
    # fail if error
    sql <- readr::read_file(query_path)
    res <- DBI::dbGetQuery(conn, sql)
    res_df <- tibble::as_tibble(res)
    
    res_df
}
