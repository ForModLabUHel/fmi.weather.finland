#' Set Up and Run Main Function
#'
#' This function clones a repository into a temporary directory, sets the working
#' directory to that folder, runs the main function with specified parameters,
#' and ensures cleanup on exit.
#'
#' @param repo_url A character string specifying the URL of the repository to clone.
#' @param branch A character string specifying the branch to clone. Default is "main".
#' @param ... Additional parameters to pass to the main function.
#' @import processx yaml checkmate
#' @return The output of the main function.
#' @export
setup_and_run <- function(repo_url, branch = "main", ...) {
  
  
  # Load required packages
  if (!requireNamespace("processx", quietly = TRUE)) install.packages("processx")
  if (!requireNamespace("yaml", quietly = TRUE)) install.packages("yaml")
  if (!requireNamespace("checkmate", quietly = TRUE)) install.packages("checkmate")
  
  library(processx)
  library(yaml)
  library(checkmate)
  
  # Input validation
  assert_string(repo_url, null.ok = FALSE)
  
  print(paste0("Url: ", repo_url))
  
  # Save the original working directory
  original_wd <- getwd()
  
  # Create a temporary directory
  local_path <- tempfile(pattern = "repo_")
  
  # Clone the repository
  tryCatch({
    clone_obj <- processx::run("git", c("clone", "-b", branch, repo_url, local_path))
    assert_directory(local_path, access = "r")
    print(paste0("Cloned repo successfully."))
  }, error = function(clone_obj) {
    message(clone_obj$stderr)
    stop("Error cloning repo!")
  })
  
  # Set the working directory to the temporary directory
  setwd(local_path)
  
  # Ensure cleanup on exit
  on.exit({
    setwd(original_wd)  # Restore the original working directory
    unlink(local_path, recursive = TRUE)  # Remove the temporary directory
  }, add = TRUE)
  
  
  assert_file_exists("r/fmi_top_level_funs.R")
  
  # Setup new environment
  print(paste0("Creating environment..."))
  env <- new.env()
  source("r/fmi_top_level_funs.R", local = env)
  env <- env$setup_new_env(env)
  
  # Function arguments
  main_function_args <- c(list(...), list(env = env))
  
  # Run the main function with parameters
  result <- do.call(env$main_function, main_function_args)
  
  return(result)
}






