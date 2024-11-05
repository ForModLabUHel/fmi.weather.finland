
#' Run a function using parallel processing (parallel library)
#'
#' @param data list List of objects to process
#' @param fun function Function to run. The first argument in fun should be the data argument passed here
#' @param cores integer Number of cores to use
#' @param libs list Libraries required by fun
#' @param sources list Source files required by fun
#' @param fun_kwargs list Keyword arguments to fun
#' @param type character Type of parallelisation to use: "FORK" (Linux and Mac), "PSOCK", "SOCK"
#'
#' @return list List of outputs corresponding to each input object
#' @export
#'
#' @examples
get_in_parallel <- function(data, fun, cores=8, libs = list(), sources = list(), fun_kwargs = list(), type = "PSOCK"){
  
  tryCatch(
    {
      print(paste0("Parallel processing..."))
      print(paste0("Type: ", type))
      cl <- parallel::makeCluster(cores, type=type, outfile="")
      registerDoParallel(cl)
      t <- system.time(
        result <- foreach(df = data) %dopar% {
          # Load libraries
          lapply(libs, require, character.only=T)
          
          # Source files
          lapply(sources, source)
          
          # Call function
          do.call(fun, c(list(df), fun_kwargs))
        }
      )
    },
    error = function(e) {
      print(e)
    },
    finally = {
      print(t)
      
      print("Done.")
      
      # Stop cluster
      on.exit(parallel::stopCluster(cl))
      
      return(result)

    }
  )
}




#' Set Up a New Environment
#'
#' This function sets up a new environment by loading configurations, sourcing files, and loading required libraries.
#'
#' @param env An environment where the configurations, sources, and libraries will be loaded.
#' @param ... Additional arguments to pass to the library loading function.
#' @return The modified environment with the loaded configurations, sources, and libraries.
#' @details 
#' This function performs the following steps:
#' \itemize{
#'   \item Validates the input environment.
#'   \item Reads the configuration file (`config.yaml`) within the provided environment.
#'   \item Sources the files specified in the configuration.
#'   \item Loads the required libraries as specified in the configuration.
#' }
#' @examples
#' \dontrun{
#' library(data.table)
#' library(sp)
#' library(sf)
#' library(yaml)
#' library(checkmate)
#' 
#' # Create a new environment
#' env <- new.env()
#' 
#' # Call the function
#' setup_new_env(env)
#' }
#' @importFrom data.table as.data.table
#' @importFrom sp coordinates proj4string
#' @importFrom sf st_as_sf
#' @importFrom yaml read_yaml
#' @importFrom checkmate assert_environment assert_file_exists
#' @export
setup_new_env <- function(env, ...) {
  # Input validations
  checkmate::assert_environment(env)
  
  with(env, {
    config_file <- "config.yaml"
    checkmate::assert_file_exists(config_file)
    config <- yaml::read_yaml(config_file)
    
    sources <- config$fmi_sources
    required_libs <- config$required_libs
    runner_path <- config$runner_path
  })
  
  # Source the files within the provided or default environment
  invisible(lapply(env$sources, function(x) source(x, local = env)))
  
  # Load the required libraries
  env$load_libraries(env$required_libs, ...)
  
  return(env)
}




#' Fetch Data from S3
#'
#' This function fetches `.nc` files from an S3 bucket, groups them based on their filenames, and returns a data.table.
#'
#' @param bucket_name A character string specifying the name of the S3 bucket.
#' @param lookup_dt_name A character string specifying the name of the lookup data table in the S3 bucket.
#' @param region A character string specifying the AWS region where the S3 bucket is located.
#' @param ... Additional arguments to pass to the `get_grouped_dt_from_filenames` function.
#' @return A data.table containing grouped filenames from the S3 bucket.
#' @details 
#' This function performs the following steps:
#' \itemize{
#'   \item Validates the existence of the S3 bucket and the lookup data table within the bucket.
#'   \item Fetches all filenames from the S3 bucket.
#'   \item Filters the filenames to include only `.nc` files.
#'   \item Groups the `.nc` files based on their filenames.
#' }
#' @examples
#' \dontrun{
#' library(data.table)
#' library(checkmate)
#' 
#' # Define S3 bucket details
#' bucket_name <- "my-bucket"
#' lookup_dt_name <- "lookup_data"
#' region <- "us-east-1"
#' 
#' # Call the function
#' nc_files_grouped_dt <- fetch_data_from_s3(bucket_name, lookup_dt_name, region)
#' print(nc_files_grouped_dt)
#' }
#' @importFrom data.table as.data.table
#' @importFrom checkmate assert_true
#' @export
fetch_data_from_s3 <- function(bucket_name, lookup_dt_name, region, ...) {
  
  print(paste0("Running fetch_data_from_s3"))
  
  # Input validations
  checkmate::assert_true(bucket_exists(bucket_name, region = region)) # Check if the S3 bucket exists
  checkmate::assert_true(head_object(lookup_dt_name, bucket = bucket_name, region = region)[[1]] == TRUE) # Check if the lookup_dt exists in the bucket
  
  filenames <- as.data.table(get_bucket_df(bucket_name, region = region))$Key
  nc_files <- filenames[str_detect(filenames, '.nc$')]
  
  args <- c(list(filenames = nc_files), ...)
  nc_files_grouped_dt <- do.call(get_grouped_dt_from_filenames, args)
  return(nc_files_grouped_dt)
}



#' Process Data
#'
#' This function processes grouped `.nc` files by filtering them for specific years, 
#' extracting requested coordinates, and preparing the necessary arguments for further processing.
#'
#' @param nc_files_grouped_dt A `data.table` containing grouped filenames from the S3 bucket.
#' @param years A numeric vector specifying the years to filter the data.
#' @param polygon A `SpatialPolygons` object representing the polygon for which data is requested.
#' @param req_coords A matrix of requested coordinates (optional).
#' @param req_nc_coords A matrix of requested netCDF coordinates (optional).
#' @param resolution A numeric value representing the resolution (in km) for the output.
#' @param fmi_allas_bucket_name A character string specifying the name of the S3 bucket.
#' @param lookup_dt_name A character string specifying the name of the lookup data table in the S3 bucket.
#' @param opts A list of additional options for S3 operations.
#' @param region A character string specifying the AWS region where the S3 bucket is located.
#' @param round_dec An integer indicating the number of decimal places to round coordinates.
#' @param join_by_vec A character vector specifying the columns to join by.
#' @return A list containing `split_dts`, a list of data.tables split by ID, and `FUN_args`, a list of arguments for further processing.
#' @details 
#' This function performs the following steps:
#' \itemize{
#'   \item Validates the input data table and numeric vector for years.
#'   \item Ensures the specified years are present in the data table.
#'   \item Reads the lookup data table from the S3 bucket.
#'   \item Extracts requested coordinates and netCDF coordinates if not provided.
#'   \item Prepares arguments for further processing.
#' }
#' @examples
#' \dontrun{
#' library(data.table)
#' library(sp)
#' library(sf)
#' library(checkmate)
#' 
#' # Example data
#' nc_files_grouped_dt <- data.table(id = 1:3, year = c(2021, 2022, 2023))
#' years <- c(2021, 2022)
#' coords <- cbind(lon = c(300000, 305000, 305000, 300000, 300000), lat = c(6800000, 6800000, 6805000, 6805000, 6800000))
#' polygon <- SpatialPolygons(list(Polygons(list(Polygon(coords)), "1")))
#' 
#' # Call the function
#' result <- process_data(nc_files_grouped_dt, years, polygon, req_coords = NULL, req_nc_coords = NULL, 
#'                        resolution = 5, fmi_allas_bucket_name = "my-bucket", lookup_dt_name = "lookup_data", 
#'                        opts = list(), region = "us-east-1", round_dec = 2, join_by_vec = c("id", "year"))
#' print(result)
#' }
#' @importFrom data.table as.data.table
#' @importFrom checkmate assert_data_table assert_numeric assert_true
#' @export
process_data <- function(nc_files_grouped_dt, years, polygon, req_coords, req_nc_coords, resolution, 
                         fmi_allas_bucket_name, lookup_dt_name, opts, region, round_dec, join_by_vec) {
  
  assert_data_table(nc_files_grouped_dt, any.missing = F)
  assert_numeric(years, any.missing = FALSE, min.len = 1)
  
  are_years_present <- all(years %in% nc_files_grouped_dt$year)
  assert_true(are_years_present)
  
  print(paste0("Running process_data..."))
  cat("\n")
  
  print(paste0("Year(s): "))
  print(paste0(years))
  cat("\n")
  
  
  char_res <- paste0(resolution, "km-by-", resolution, "km")
  print(paste0("Resolution is ", char_res, "."))
  cat("\n")
  
  filtered_fmi_lookup_dt <- get_lookup_dt_with_res_from_bucket(resolution = resolution, FUN = load_rdata_file, 
                                                               bucket = fmi_allas_bucket_name, object = lookup_dt_name, 
                                                               opts = list(region = region))
  
  if(is.null(req_coords)) {
    req_coords <- extract_polygon_coords_with_res(polygon = polygon, reference_dt = filtered_fmi_lookup_dt, 
                                                  resolution = resolution)
  }
  
  
  if(is.null(req_nc_coords)) {
    req_nc_coords <- get_req_nc_coords(req_coords, reference_coords_dt = filtered_fmi_lookup_dt, round_dec = round_dec, 
                                       is_longlat = FALSE)
  }
  
  req_coords_lookup_dt <- create_clim_id_lookup_dt(req_coords, req_nc_coords)
  req_nc_coords <- unique(req_nc_coords)
  
  s3read_args <- list(FUN = nc_open, bucket = fmi_allas_bucket_name, opts = c(opts, list(region=region)))
  extract_nc_vars_by_polygon_coords_args <- list(polygon = polygon, req_coords = req_coords, 
                                                 time_var = "Time", x_var = "Lon", y_var = "Lat", 
                                                 is_longlat = FALSE, req_nc_coords = req_nc_coords)
  
  FUN_args = list(s3read_args = s3read_args, extract_nc_vars_by_polygon_coords_args = extract_nc_vars_by_polygon_coords_args)
  dt_years <- nc_files_grouped_dt[year %in% years]
  split_dts <- split(dt_years, by = "id")
  
  
  return(list(split_dts = split_dts, FUN_args = FUN_args, req_coords_lookup_dt = req_coords_lookup_dt))
  
}



#' Get FMI Data from Allas
#'
#' This function fetches data from the Allas service for the Finnish Meteorological Institute (FMI) using a polygon or requested coordinates.
#'
#' @param polygon A `SpatialPolygons` object defining the area of interest. Default is `NULL`.
#' @param req_coords A matrix of requested coordinates. Default is `NULL`.
#' @param req_nc_coords A matrix of requested NetCDF coordinates. Default is `NULL`.
#' @param round_dec A numeric value specifying the decimal places to round coordinates. Default is `3`.
#' @param years A numeric vector specifying the years of interest. Default is `c(1961)`.
#' @param resolution A numeric value specifying the resolution in km. Default is `1`.
#' @param fmi_allas_bucket_name A character string specifying the name of the Allas bucket. Default is `"2000994-fmi"`.
#' @param lookup_dt_name A character string specifying the name of the lookup data table file. Default is `"fmi_resolutions_1_5_9_lookup_dt.rdata"`.
#' @param join_by_vec A character vector specifying the columns to join by. Default is `c("id", "time", "x", "y")`.
#' @param opts A list of additional options for the function. Default is `NULL`.
#' @return A list containing the split data tables and function arguments for further processing.
#' @details 
#' This function performs the following steps:
#' \itemize{
#'   \item Validates that either `polygon` or `req_coords` is provided.
#'   \item Sets the AWS region for the session.
#'   \item Fetches grouped NetCDF files from the Allas bucket.
#'   \item Processes the data based on the provided parameters and configurations.
#' }
#' @examples
#' \dontrun{
#' library(data.table)
#' library(sp)
#' library(sf)
#' library(yaml)
#' library(checkmate)
#' 
#' # Define example parameters
#' polygon <- NULL
#' req_coords <- NULL
#' req_nc_coords <- NULL
#' round_dec <- 3
#' years <- c(1961, 1962)
#' resolution <- 1
#' fmi_allas_bucket_name <- "2000994-fmi"
#' lookup_dt_name <- "fmi_resolutions_1_5_9_lookup_dt.rdata"
#' join_by_vec <- c("id", "time", "x", "y")
#' opts <- list(region = "eu-west-1")
#' 
#' # Call the function
#' result <- get_fmi_data_from_allas(polygon, req_coords, req_nc_coords, round_dec, years, resolution, 
#'                                   fmi_allas_bucket_name, lookup_dt_name, join_by_vec, opts)
#' print(result)
#' }
#' @importFrom data.table as.data.table
#' @importFrom sp coordinates proj4string
#' @importFrom sf st_as_sf
#' @importFrom yaml read_yaml
#' @importFrom checkmate assert_class assert_file_exists
#' @export
get_fmi_data_from_allas <- function(polygon = NULL, 
                                    req_coords = NULL, 
                                    req_nc_coords = NULL, 
                                    round_dec = 3, 
                                    years = c(1961), 
                                    resolution = 1, 
                                    fmi_allas_bucket_name = "2000994-fmi", 
                                    lookup_dt_name = "fmi_resolutions_1_5_9_lookup_dt.rdata",
                                    join_by_vec = c("id", "time", "x", "y"),
                                    opts = NULL) {
  
  if (is.null(polygon) && is.null(req_coords)) stop("Either polygon or req_coords must be provided.")
  
  with_envvar(opts, {
    # Set region for session
    region <- Sys.getenv("AWS REGION")
    
    get_grouped_dt_from_filenames_args = list(column_names = c("var", "year"), group_vars = c("year"))
    
    nc_files_grouped_dt <- fetch_data_from_s3(fmi_allas_bucket_name, lookup_dt_name, region, get_grouped_dt_from_filenames_args)
    return_list <- process_data(nc_files_grouped_dt, years, polygon, req_coords, req_nc_coords, resolution, 
                                fmi_allas_bucket_name, lookup_dt_name, opts, region, round_dec, join_by_vec)
  })
  
  return(return_list)
}



#' Get NetCDF Variables in Parallel
#'
#' This function processes NetCDF files in parallel using the provided data and function arguments.
#'
#' @param return_list A list containing the split data tables and function arguments for processing.
#' @param opts A list of additional options for the function. Default is `NULL`.
#' @return A list of data.tables containing the processed NetCDF variables.
#' @details 
#' This function performs the following steps:
#' \itemize{
#'   \item Determines the number of available cores for parallel processing.
#'   \item Processes the split data tables in parallel using the provided function and arguments.
#' }
#' @examples
#' \dontrun{
#' library(data.table)
#' library(parallel)
#' 
#' # Define example return_list
#' return_list <- list(
#'   split_dts = list(
#'     data.table(id = 1, filename = "file1.nc"),
#'     data.table(id = 2, filename = "file2.nc")
#'   ),
#'   FUN_args = list(
#'     s3read_args = list(FUN = nc_open, bucket = "2000994-fmi", opts = list(region = "eu-west-1")),
#'     extract_nc_vars_by_polygon_coords_args = list(
#'       polygon = NULL, req_coords = NULL, time_var = "Time", x_var = "Lon", y_var = "Lat",
#'       is_longlat = FALSE, req_nc_coords = NULL
#'     )
#'   )
#' )
#' 
#' # Call the function
#' result <- get_nc_vars_parallel(return_list)
#' print(result)
#' }
#' @importFrom data.table as.data.table
#' @importFrom parallel availableCores
#' @export
get_nc_vars_parallel <- function(return_list, opts = NULL){
  split_dts <- return_list$split_dts
  FUN_args <- return_list$FUN_args
  join_by_vec = c("id", "time", "x", "y") 
  
  n_cores <- as.integer(min(length(split_dts), max(1, availableCores() - 1)))
  
  print(paste0("Running on ", n_cores, " core(s)."))
  cat("\n")
  
  print(paste0("Processing nc files: "))
  cat("\n")
  
  with_envvar(opts, {
    var_dts <- get_in_parallel(data = split_dts,
                               fun = process_from_grouped_dt_and_join,
                               cores = n_cores,
                               fun_kwargs = list(FUN = get_nc_vars_from_bucket,
                                                 FUN_args = FUN_args,
                                                 process_var_name = "filename",
                                                 join_by_vec = join_by_vec),
                               type = "FORK")
  })
  
  return(var_dts)
}



#' Main Function to Set Up Environment and Run Scripts
#'
#' This function sets up an environment, validates inputs, and runs a script in a new process using `processx`.
#'
#' @param env An environment where configurations and data will be loaded.
#' @param save_path A character string specifying the directory to save the result table. Default is the current working directory.
#' @param opts A list of additional options for the function. Default is `NULL`.
#' @param ... Additional parameters to pass to the `get_fmi_data_from_allas` function.
#' @return This function does not return a value but sets up the environment and runs a script in a new process.
#' @details 
#' This function performs the following steps:
#' \itemize{
#'   \item Validates the input environment and the provided directory for saving files.
#'   \item Calls the `get_fmi_data_from_allas` function with the provided parameters and options.
#'   \item Adds objects to the environment for further processing.
#'   \item Saves the return list to a temporary file and ensures cleanup on exit.
#'   \item Runs a script in a new process using `processx` with the environment setup.
#' }
#' @examples
#' \dontrun{
#' library(data.table)
#' library(checkmate)
#' 
#' # Create a new environment
#' env <- new.env()
#' env$runner_path <- "path/to/runner_script.R"
#' 
#' # Call the main function
#' main_function(env, save_path = "path/to/save", opts = list(region = "eu-west-1"))
#' }
#' @importFrom data.table as.data.table
#' @importFrom checkmate assert_environment assert_directory assert_file_exists
#' @importFrom processx run
#' @export
main_function <- function(env,
                          save_path = getwd(),
                          opts = NULL,
                          ...) {
  
  # Input validations
  checkmate::assert_environment(env)
  checkmate::assert_directory(save_path, access = "rw")
  checkmate::assert_file_exists(env$runner_path)
  
  # Get params for function
  args <- list(...)
  
  # Call the get_fmi_data_from_allas with the provided params
  return_list <- do.call(get_fmi_data_from_allas, c(args, list(opts = opts)))
  
  # Add objects to env
  data <- list(return_list = return_list, opts = opts, save_path = save_path)
  env$data <- data
  
  # Save the return_list to a temporary file and unlink it on exit
  temp_file <- tempfile(fileext = ".rds")
  saveRDS(list(env = env), file = temp_file)
  on.exit(unlink(temp_file), add = TRUE)
  checkmate::assert_file_exists(temp_file)
  
  # Call the script that runs the parallel function using processx
  tryCatch({
    runner_obj <- processx::run("Rscript", c(env$runner_path, temp_file), echo = TRUE)
  }, error = function(runner_obj) {
    message(runner_obj$stderr)
    stop(paste0("Error running ", env$runner_path))
  })
}


















