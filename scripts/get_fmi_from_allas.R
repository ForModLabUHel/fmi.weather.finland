# This script is for extracting FMI meteorological data that is stored in Allas.
# The script must be run on Puhti. First load and run the fetch_file_from_github function
# to load the setup_and_run function. Then provide the parameters and run with either a
# set of requested coordinates or a polygon.

library(data.table)
library(sp)


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
rm(init_funs, file_path, repo)

### ------------------------------------------------------------------------ ###




# SET PARAMETERS

resolution <- 5 # Resolution in km (1, 5 or 9)
years <- c(1961) # For which years to extract (1961:2023 are full years)
save_path <- paste0(getwd()) # Where to save the extracted data.table as .rdata
repo_url <- "https://github.com/ForModLabUHel/fmi.weather.finland.git" # Project repository to use





### ----------------------- EXAMPLE WITH COORDS ---------------------------- ###


example_req_coords_dt <- data.table(
  id = 1:25,
  E = c(
    300189.9, 301189.9, 302189.9, 303189.9, 304189.9, 
    300189.9, 301189.9, 302189.9, 303189.9, 304189.9, 
    300189.9, 301189.9, 302189.9, 303189.9, 304189.9, 
    300189.9, 301189.9, 302189.9, 303189.9, 304189.9, 
    300189.9, 301189.9, 302189.9, 303189.9, 304189.9
  ),
  N = c(
    6804275, 6804275, 6804275, 6804275, 6804275, 
    6803275, 6803275, 6803275, 6803275, 6803275, 
    6802275, 6802275, 6802275, 6802275, 6802275, 
    6801275, 6801275, 6801275, 6801275, 6801275, 
    6800275, 6800275, 6800275, 6800275, 6800275
  )
)


req_coords <- as.matrix(example_req_coords_dt[, c("E", "N")]) # The coords are passed as a matrix

# Set parameters
params <- list(req_coords = req_coords, resolution = resolution, years = years)

# Combine arguments
setup_and_run_args <- c(params, list(save_path = save_path, repo_url = repo_url, branch = branch))

# RUN
result <- do.call(setup_and_run, setup_and_run_args)

### ------------------------------------------------------------------------ ###






### ----------------------- EXAMPLE WITH POLYGON --------------------------- ###

# Coordinates form a closed loop
WGScoor <- data.table(lon = c(300000, 305000, 305000, 300000, 300000),
                      lat = c(6800000, 6800000, 6805000, 6805000, 6800000))

# Convert to matrix
coords_matrix <- as.matrix(WGScoor)

# Create a Polygon
polygon <- Polygon(coords_matrix)

# Create Polygons object
polygons <- Polygons(list(polygon), ID = "1")

# Create SpatialPolygons object with CRS
spatial_polygons <- SpatialPolygons(list(polygons), proj4string = CRS("EPSG:3067"))

# Set parameters
params <- list(polygon = spatial_polygons, resolution = resolution, years = years)

# Combine arguments
setup_and_run_args <- c(params, list(save_path = save_path, repo_url = repo_url, branch = branch))

# RUN
result <- do.call(setup_and_run, setup_and_run_args)


### ------------------------------------------------------------------------ ###






