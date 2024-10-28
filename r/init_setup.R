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
  
  message("Url: ", repo_url)
  
  # Save the original working directory
  original_wd <- getwd()
  
  # Create a temporary directory
  local_path <- tempfile(pattern = "repo_")
  
  # Clone the repository
  tryCatch({
    clone_obj <- processx::run("git", c("clone", "-b", branch, repo_url, local_path))
    assert_directory(local_path, access = "r")
  }, error = function(clone_obj) {
    message("Error cloning repo!")
    message(clone_obj$stderr)
  }, finally = {
    # Check if cloning was successful
    if (dir.exists(local_path)) {
      print(paste0("Cloned repo successfully."))
    }
  })
  
  # Set the working directory to the temporary directory
  setwd(local_path)
  
  # Ensure cleanup on exit
  on.exit({
    setwd(original_wd)  # Restore the original working directory
    unlink(local_path, recursive = TRUE)  # Remove the temporary directory
  }, add = TRUE)
  
  # Load environment
  if (file.exists("scripts/settings.R")) {
    source("scripts/settings.R")
  } else {
    stop("Error: settings.R not found.")
  }
  
  # Function arguments
  main_function_args <- c(list(...), list(runner_path = runner_path))
  
  # Run the main function with parameters
  result <- do.call(main_function, main_function_args)
  
  return(result)
}



test_setup_and_run <- function(repo_url, ...) {
  
  # Load required packages
  library(processx)
  library(yaml)
  library(checkmate)
  
  # Input validation
  assert_string(repo_url, null.ok = FALSE)
  
  print(paste0("Url: ", repo_url))
  
  # Save the original working directory
  original_wd <- getwd()
  
  # Set the working directory to the temporary directory
  setwd(original_wd)
  
  # Load environment
  source("scripts/settings.R")
  
  # Function arguments
  main_function_args <- c(list(...), list(runner_path = runner_path))
  
  # Run the main function with parameters
  result <- do.call(main_function, main_function_args)
  
  return(result)
}





