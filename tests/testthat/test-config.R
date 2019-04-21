context("test-config")


setup({
    
    config_file <- tempfile(pattern = "config-", fileext = "yml")
    config_lst <- list(
        default = list(
            config_hash = "default",
            debug = TRUE
        ),
        test = list(
            config_hash = config_get_hash(),
            debug = FALSE
        )
    )
    
    yaml::write_yaml(config_lst, config_file)
    assign("config_file", config_file, envir = .GlobalEnv)
})



test_that("hashing works", {
    
    hash <- config_get_hash()
    expect_match(hash, "[0-9a-f]{16,}")
})


test_that("config works", {
    
    cfg <- config_load(config_active = "default", config_file = config_file)
    expect_identical(cfg$config_hash, "default")
    expect_identical(cfg$debug, TRUE)
    
    cfg <- config_load(config_active = NULL, config_file = config_file)
    expect_identical(cfg$config_hash, config_get_hash())
    expect_identical(cfg$debug, FALSE)
})



teardown({
    
    unlink(config_file, recursive = TRUE, force = TRUE)
    
    base::rm(list = "config_file", envir = .GlobalEnv)
})
