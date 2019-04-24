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
            pk_pattern = connector_lst$pk_pattern,
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
            if (is_key_chr(attr_value)) {
                attr(.PKGENV$POOL[[connector]], attr_name) <- attr_value
            }
        })
    
    invisible(NULL)
}


db_get_attr <- function(attr_name,
                        connector = NULL) {
    
    pl <- db_get_pool(connector = connector)
    
    attr(pl, attr_name)
}


db_get_database <- function(connector = NULL) {
    
    db_get_attr("database", connector = connector)
}


db_get_schema <- function(connector = NULL) {
    
    db_get_attr("schema", connector = connector)
}


# bin ----
to_bin <- function(x) {
    
    as.raw(serialize(x, NULL))
}


to_bin_lst <- function(x) {
    
    purrr::map(x, to_bin)
}


to_bin_df <- function(df, 
                      bin_cols = ends_with("_bin")) {
    
    msk <- rlang::has_name(df, bin_cols)
    cols <- cols[msk]
    if (length(cols) == 0L) return(df)
    
    bin_cols <- glue::glue("{cols}_bin")
    
    df %>%
        dplyr::mutate_at(cols, to_bin_lst) %>%
        dplyr::rename_at(dplyr::vars(cols), ~ bin_cols)
}


from_bin <- function(x) {
    
    unserialize(as.raw(x))
}


from_bin_lst <- function(x) {
    
    purrr::map(x, from_bin)
}


from_bin_df <- function(df,
                        bin_cols = NULL) {
    
    if (is.null(bin_cols)) {
        msk <- get_classes(df) == "raw"
        bin_cols <- names(df)[msk]
    } else {
        msk <- rlang::has_name(df, bin_cols)
        bin_cols <- bin_cols[msk]
    }
    if (length(bin_cols) == 0L) return(df)
    
    df %>%
        dplyr::mutate_at(bin_cols, from_bin_lst) %>%
        dplyr::rename_at(dplyr::vars(bin_cols), ~ cols)
}


# tbl ----
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


db_require_tbl <- function(table,
                           tables_only = FALSE,
                           schema = NULL,
                           connector = NULL) {
    
    stopifnot(db_tbl_exists(
        table = table, tables_only = tables_only,
        schema = schema, connector = connector))
}


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


db_collect <- function(tbl,
                       bin_cols = NULL) {
    
    col_names <- colnames(tbl)
    
    if (is.null(bin_cols)) {
        msk <- grepl("_bin$", col_names)
        bin_cols <- col_names[msk]
    } else {
        bin_cols <- bin_cols %if_in% col_names
    }
    
    if (length(bin_cols) > 0L) {
        tbl %>%
            # _bin are the last columns when collecting
            dplyr::select(-dplyr::one_of(bin_cols), dplyr::everything()) %>%
            dplyr::collect() %>%
            # restore col order
            dplyr::select(dplyr::one_of(col_names)) %>%
            from_bin_df()
    } else {
        dplyr::collect(tbl)
    }
}


# field ----
db_list_fields <- function(table,
                           schema = NULL,
                           connector = NULL) {
    
    schema <- schema %||% db_get_schema(connector = connector)
    
    conn <- pool::poolCheckout(db_connect(connector = connector))
    on.exit(pool::poolReturn(conn))
    
    odbc::dbListFields(conn = conn, name = table, schema_name = schema)
}


db_get_pk <- function(table,
                      schema = NULL,
                      connector = NULL) {
    
    pl <- db_get_pool(connector = connector)
    schema <- schema %||% attr(pl, "schema")
    pk_pattern <- attr(pl, "pk_pattern")
    
    # default pattern uses var `table``
    glue::glue(pk_pattern)
}


db_get_max <- function(field,
                       table,
                       schema = NULL,
                       connector = NULL) {
    
    val <- NULL
    
    stopifnot(is_key_chr(field_name))
    
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


# low level ----
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
    sql <- glue::glue(
        "INSERT INTO [{schema}].[{table}] (",
        paste0(df_fields, collapse = ", "), ")\n",
        "VALUES (", paste0(params, collapse = ", "), ")"
    )
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
    sql <- glue::glue(
        "UPDATE [{schema}].[{table}]",
        " SET ", paste0(df_fields[-1L], "=?", collapse = ", "),
        " WHERE {df_fields[1L]}=?", 
    )
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


db_get_max <- function(field,
                       table, 
                       schema = NULL,
                       connector = NULL) {
    
    val <- NULL
    stopifnot(is_key_chr(field_name))
    
    df <- 
        db_tbl(table = table, schema = schema, connector = connector) %>%
        dplyr::select(val = dplyr::one_of(field)) %>%
        dplyr::summarise(val = max(val, na.rm = TRUE)) %>%
        dplyr::collect()
    
    if (identical(df$val, NA_integer_)) {
        0L
    } else {
        df$val
    }
}
