
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



#' Process Data from NetCDF Files
#'
#' This function processes data from grouped NetCDF files, prepares arguments for further processing, and handles coordinate precedence.
#'
#' @param nc_files_grouped_dt A data.table of grouped NetCDF files.
#' @param years A numeric vector of years to process.
#' @param polygon A SpatialPolygons object representing the area of interest. Default is `NULL`.
#' @param req_coords A matrix of requested coordinates. Default is `NULL`.
#' @param req_nc_coords A matrix of requested NetCDF coordinates. Default is `NULL`.
#' @param resolution A numeric value indicating the resolution.
#' @param fmi_allas_bucket_name A character string specifying the name of the S3 bucket.
#' @param lookup_dt_name A character string specifying the name of the lookup data table.
#' @param opts A list of options for the function. Default is `NULL`.
#' @param region A character string specifying the AWS region.
#' @param round_dec A numeric value indicating the number of decimal places to round to.
#' @param join_by_vec A character vector specifying the columns to join by.
#' @return A list containing the split data tables, function arguments, and the lookup data table.
#' @importFrom data.table assert_data_table
#' @importFrom ncdf4 nc_open nc_close
#' @export
process_data <- function(nc_files_grouped_dt, years, polygon, req_coords, req_nc_coords, resolution, 
                         fmi_allas_bucket_name, lookup_dt_name, opts, region, round_dec, join_by_vec) {
  # Input validations
  assert_data_table(nc_files_grouped_dt, any.missing = FALSE)
  assert_numeric(years, any.missing = FALSE, min.len = 1)
  are_years_present <- all(years %in% nc_files_grouped_dt$year)
  assert_true(are_years_present)
  
  # Print messages 
  print_process_data_messages(years, resolution)
  
  # Load filtered lookup data table
  filtered_fmi_lookup_dt <- get_lookup_dt_with_res_from_bucket(resolution = resolution, FUN = load_rdata_file, 
                                                               bucket = fmi_allas_bucket_name, object = lookup_dt_name, 
                                                               opts = list(region = region))
  
  # Handle coordinates precedence 
  coords <- handle_process_data_input_coords(polygon, req_coords, req_nc_coords, 
                                             filtered_fmi_lookup_dt, resolution, round_dec) 
  req_coords <- coords$req_coords 
  req_nc_coords <- coords$req_nc_coords 
  req_coords_lookup_dt <- coords$req_coords_lookup_dt
  
  # Prepare arguments for further processing
  s3read_args <- list(FUN = nc_open, bucket = fmi_allas_bucket_name, opts = c(opts, list(region = region)))
  extract_nc_vars_by_coords_args <- list(req_coords = req_coords, 
                                         time_var = "Time", 
                                         x_var = "Lon", 
                                         y_var = "Lat", 
                                         is_longlat = FALSE, 
                                         req_nc_coords = req_nc_coords,
                                         round_dec = round_dec)
  
  # Split data and prepare function arguments
  FUN_args = list(s3read_args = s3read_args, extract_nc_vars_by_coords_args = extract_nc_vars_by_coords_args)
  dt_years <- nc_files_grouped_dt[year %in% years]
  split_dts <- split(dt_years, by = "id")
  
  return(list(split_dts = split_dts, FUN_args = FUN_args, req_coords_lookup_dt = req_coords_lookup_dt))
}



#' Get FMI Data from ALLAS
#'
#' This function retrieves FMI data from ALLAS based on provided coordinates or polygon, and processes the data.
#'
#' @param polygon A SpatialPolygons object representing the area of interest. Default is `NULL`.
#' @param req_coords A matrix of requested coordinates. Default is `NULL`.
#' @param req_nc_coords A matrix of requested NetCDF coordinates. Default is `NULL`.
#' @param round_dec A numeric value indicating the number of decimal places to round to. Default is 3.
#' @param years A numeric vector of years to process. Default is `c(1961)`.
#' @param resolution A numeric value indicating the resolution. Default is 1.
#' @param opts A list of options for the function. Default is `NULL`.
#' @param config A configuration object. Default is `NULL`.
#' @return A list containing the processed data.
#' @importFrom checkmate assert_list assert_names
#' @export
get_fmi_data_from_allas <- function(polygon = NULL, 
                                    req_coords = NULL, 
                                    req_nc_coords = NULL, 
                                    round_dec = 3, 
                                    years = c(1961), 
                                    resolution = 1,
                                    opts = NULL,
                                    config = NULL) {
  if (is.null(req_coords) && is.null(req_nc_coords) && is.null(polygon)) {
    stop("Either req_coords or req_nc_coords must be provided, or polygon must be provided.")
  }
  
  if (is.null(config)) {
    stop("No config file provided!")
  }
  
  # Validate configuration object
  assert_list(config, null.ok = FALSE)
  assert_names(names(config), must.include = c("fmi_allas_bucket_name", "lookup_dt_name", "join_by_vec"))
  
  # Extract necessary values from config
  fmi_allas_bucket_name <- config$fmi_allas_bucket_name
  lookup_dt_name <- config$lookup_dt_name
  join_by_vec <- config$join_by_vec
  
  # Handle precedence using the helper function
  coords <- handle_coords_input_precedence(polygon, req_coords, req_nc_coords)
  polygon <- coords$polygon
  req_coords <- coords$req_coords
  req_nc_coords <- coords$req_nc_coords
  
  # Get region
  region <- Sys.getenv("AWS_REGION")
  
  get_grouped_dt_from_filenames_args = list(column_names = c("var", "year"), group_vars = c("year"))
  
  nc_files_grouped_dt <- fetch_data_from_s3(fmi_allas_bucket_name, lookup_dt_name, region, get_grouped_dt_from_filenames_args)
  return_list <- process_data(nc_files_grouped_dt, years, polygon, req_coords, req_nc_coords, resolution, 
                              fmi_allas_bucket_name, lookup_dt_name, opts, region, round_dec, join_by_vec)
  
  return(return_list)
}



#' Get NetCDF Variables in Parallel
#'
#' This function processes NetCDF variables in parallel using multiple cores.
#'
#' @param return_list A list containing the split data tables and function arguments.
#' @param opts A list of options for the function. Default is `NULL`.
#' @return A data.table containing the processed NetCDF variables.
#' @importFrom parallel availableCores
#' @export
get_nc_vars_parallel <- function(return_list, opts = NULL) {
  split_dts <- return_list$split_dts
  FUN_args <- return_list$FUN_args
  join_by_vec = c("id", "time", "x", "y") 
  
  n_cores <- as.integer(min(length(split_dts), max(1, availableCores() - 1)))
  
  print(paste0("Running on ", n_cores, " core(s)."))
  cat("\n")
  
  print(paste0("Processing nc files: "))
  cat("\n")
  
  var_dts <- get_in_parallel(data = split_dts,
                             fun = process_from_grouped_dt_and_join,
                             cores = n_cores,
                             fun_kwargs = list(FUN = get_nc_vars_from_bucket,
                                               FUN_args = FUN_args,
                                               process_var_name = "filename",
                                               join_by_vec = join_by_vec),
                             type = "FORK")
  
  return(var_dts)
}






#' Main Function to Set Up Environment and Run Scripts
#'
#' This function sets up an environment, validates inputs, and runs a script in a new process using `processx`.
#'
#' @param env An environment where configurations and data will be loaded.
#' @param save_path A character string specifying the directory to save temporary files. Default is the current working directory.
#' @param opts A list of additional options for the function. Default is `NULL`.
#' @param ... Additional parameters to pass to the `get_fmi_data_from_allas` function.
#' @return This function does not return a value but sets up the environment and runs a script in a new process.
#' @importFrom checkmate assert_environment assert_directory assert_file_exists assert_list
#' @importFrom processx run
#' @export
main_function <- function(env,
                          save_path = getwd(),
                          opts = NULL,
                          ...) {
  # Input validations
  assert_environment(env)
  assert_directory(save_path, access = "rw")
  assert_file_exists(env$runner_path)
  assert_list(opts, null.ok = TRUE)
  
  # Get params for function
  args <- list(...)
  
  # Call the get_fmi_data_from_allas with the provided params
  return_list <- do.call(get_fmi_data_from_allas, c(args, list(opts = opts, config = env$config)))
  
  # Add objects to env
  data <- list(return_list = return_list, opts = opts, save_path = save_path)
  env$data <- data
  
  # Save the return_list to a temporary file and unlink it on exit
  temp_file <- tempfile(fileext = ".rds")
  saveRDS(list(env = env), file = temp_file)
  on.exit(unlink(temp_file), add = TRUE)
  assert_file_exists(temp_file)
  
  # Call the script that runs the parallel function using processx
  tryCatch({
    runner_obj <- processx::run("Rscript", c(env$runner_path, temp_file), echo = TRUE)
  }, error = function(runner_obj) {
    message(runner_obj$stderr)
    stop(paste0("Error running ", env$runner_path))
  })
}



















