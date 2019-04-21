#' re-export magrittr pipe operator
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
#' @export
rlang::`%||%`


# allows using .data in tidyverse pipelines
#' @importFrom rlang .data
NULL

# quiets concerns of R CMD check re: the .'s that appear in pipelines
if (getRversion() >= "2.15.1")  utils::globalVariables(c("."))
