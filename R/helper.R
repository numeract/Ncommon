# general helper functions

# !diagnostics suppress=


# operators ----

#' @export
`%|||%` <- function(x, y) {
    if (length(x) == 0L || all(is.na(x))) y else x
}


# is ----

#' @export
is_boolean <- function(x) {
    
    rlang::is_scalar_logical(x) && !is.na(x)
}


# is_key ----

#' @export
is_key_chr <- function(x) {
    
    rlang::is_string(x) && (nchar(x) > 0L)
}


#' @export
is_key_int <- function(x, min_val = 1) {
    
    rlang::is_bare_integerish(x, n = 1L) && !is.na(x) && (x >= min_val)
}


#' @export
is_key_Date <- function(x) {
    
    inherits(x, "Date") && length(x) == 1L && !is.na(x)
}


# are_keys ----

#' @export
are_keys_chr <- function(x) {
    
    rlang::is_character(x) && length(x) > 0L && !anyNA(x) && all(nchar(x) > 0L)
}


#' @export
are_keys_int <- function(x, min_val = 1) {
    
    rlang::is_bare_integerish(x) && length(x) > 0L && !anyNA(x) && all(x >= min_val)
}


#' @export
are_keys_Date <- function(x) {
    
    inherits(x, "Date") && length(x) > 0L && !anyNA(x)
}


# composite_key ----

#' @export
has_composite_key <- function(.data, ...) {
    
    dplyr::n_distinct(dplyr::select(.data, ...)) == nrow(.data)
}


#' @export
require_composite_key <- function(.data, ...) {
    
    if (!has_composite_key(.data, ...)) {
        col_names <- paste0(names(dplyr::select(.data, ...)), collapse = ", ")
        rlang::abort(glue::glue(
            "composite key failed for: {col_names}"))
    } else {
        TRUE
    }
}


# class ----

#' @export
get_class <- function(x) {
    
    class(x)[1L]
}


#' @export
get_classes <- function(x) {
    
    if (is.data.frame(x) || is.list(x)) {
        purrr::map_chr(x, ~ class(.x[[1L]])[1L])
    } else {
        rlang::abort("x is not a list or a data frame")
    }
}


#' @export
set_class <- function(x, class_names) {
    
    stopifnot(is.null(class_names) || are_keys_chr(class_names))
    class(x) <- class_names
    
    x
}


#' @export
prepend_class <- function(x, class_names) {
    
    stopifnot(are_keys_chr(class_names))
    class(x) <- c(class_names, class(x))
    
    x
}


#' @export
require_class <- function(var, class_name) {
    
    if (!rlang::inherits_any(var, class_name)) {
        mc <- match.call()
        msg <- paste0("`", mc[[2]], "` must be of class `", class_name, "`.")
        rlang::abort(msg)
    }
}


# date time ----

#' @export
as_dttm_utc <- function(x, input_tz = "", ...) {
    
    dt <- as.POSIXct(x, tz = input_tz, ...)
    attr(dt, "tzone") <- "UTC"
    
    dt
}


#' @export
is_week_day <- function(x, six_days = FALSE) {
    
    if (six_days) {
        weekend <- 7L
    } else {
        weekend <- 6:7
    }
    
    !(strftime(x, '%u') %in% weekend)
}
