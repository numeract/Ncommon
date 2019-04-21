#' Re-export magrittr pipe operator
#' 
#' @importFrom magrittr %>%
#' @name %>%
#' @rdname pipe
#' @export
NULL


#' Default value for `NULL`
#' 
#' @importFrom rlang %||%
#' @keywords NULL
#' @name op-null-default
#' @rdname op-null-default
#' @export
rlang::`%||%`


#' Keep elements present in x and not contained in y
#' 
#' @importFrom Nmisc %if_in%
#' @name %if_in%
#' @rdname keep_if_in
#' @export
NULL


#' Discard elements present in x and not contained in y
#' 
#' @importFrom Nmisc %if_not_in%
#' @name %if_not_in%
#' @rdname keep_if_not_in
#' @export
NULL


#' Multiple assignment operator
#' 
#' Avoid conflict with future::`\%<-\%`.
#' 
#' @param x A name structure, see section below.
#' @param value A list of values, vector of values, or \R object to assign.
#' 
#' @name %<=%
#' @rdname multiple_assignment
#' @export
`%<=%` <- zeallot::`%<-%`


# allows using .data in tidyverse pipelines
#' @importFrom rlang .data
NULL


# quiets concerns of R CMD check re: the .'s that appear in pipelines
utils::globalVariables(c("."))
