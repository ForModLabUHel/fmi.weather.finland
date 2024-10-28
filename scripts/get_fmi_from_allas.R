# This script is for extracting FMI meteorological data that is stored in Allas.
# The script only works when running on Puhti.


### ------------------------------ RUN FIRST ------------------------------- ###


fetch_file_from_github <- function(repo, file_path, branch = "main") {
  library(gh)
  library(base64enc)

  tryCatch({
    # Fetch the file content
    file_content <- gh::gh(
      "/repos/{owner}/{repo}/contents/{path}",
      owner = strsplit(repo, "/")[[1]][1],
      repo = strsplit(repo, "/")[[1]][2],
      path = file_path,
      ref = branch
    )
    # Decode content from base64
    content <- rawToChar(base64enc::base64decode(file_content$content))
  }, error = function(e) {
    message("Error fetching the file content: ", e)
  })

  return(content)
}

repo <- "ForModLabUHel/fmi.weather.finland"
file_path <- "r/init_setup.R"
branch <- "fmi-from-allas"

# Get init functions from github
init_funs <- fetch_file_from_github(repo, file_path, branch)
eval(parse(text = init_funs))


### ------------------------------------------------------------------------ ###




library(data.table)

example_req_coords_dt <- data.table(
  id = 1:7,
  E = c(95000, 255000, 385000, 465000, 505000, 505000, 505000),
  N = c(6705000, 6765000, 6805000, 6945000, 7145000, 7335000, 7565000),
  x = c(19.673, 22.480, 24.851, 26.318, 27.104, 27.111, 27.120),
  y = c(60.295, 60.959, 61.377, 62.647, 64.441, 66.143, 68.203)
)


req_coords <- as.matrix(example_req_coords_dt[, c("E", "N")]) # The coords are passed as a matrix

# Set parameters
params <- list(resolution = 1, req_coords = req_coords, years = c(1961:1962))

save_path <- getwd()
repo_url <- "https://github.com/ForModLabUHel/fmi.weather.finland.git"

setup_and_run_args <- c(params, list(save_path = save_path, repo_url = repo_url, branch = branch))

# RUN
result <- do.call(setup_and_run, setup_and_run_args)






