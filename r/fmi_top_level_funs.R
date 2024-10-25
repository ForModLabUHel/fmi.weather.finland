
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




setup_environment <- function(sources, required_libs, ...) {
  print(paste0("Running setup_environment..."))
  
  invisible(lapply(sources, source))
  load_libraries(required_libs, ...)
}



fetch_data_from_s3 <- function(bucket_name, lookup_dt_name, region, ...) {
  
  print(paste0("Running fetch_data_from_s3"))
  
  # Input validations
  assert_true(bucket_exists(bucket_name, region = region)) # Check if the S3 bucket exists
  assert_true(head_object(lookup_dt_name, bucket = bucket_name, region = region)[[1]] == TRUE) # Check if the lookup_dt exists in the bucket
  
  filenames <- as.data.table(get_bucket_df(bucket_name, region = region))$Key
  nc_files <- filenames[str_detect(filenames, '.nc$')]
  
  args <- c(list(filenames = nc_files), ...)
  nc_files_grouped_dt <- do.call(get_grouped_dt_from_filenames, args)
  return(nc_files_grouped_dt)
}


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
  
  fmi_lookup_dt <- get_lookup_dt_with_res_from_bucket(resolution = resolution, FUN = load_rdata_file, 
                                                      bucket = fmi_allas_bucket_name, object = lookup_dt_name, 
                                                      opts = list(region = region))
  
  
  if(is.null(req_nc_coords)) {
    req_nc_coords <- get_req_nc_coords(req_coords, reference_coords_dt = fmi_lookup_dt, round_dec = round_dec, 
                                       is_longlat = FALSE)
  }
  
  s3read_args <- list(FUN = nc_open, bucket = fmi_allas_bucket_name, opts = c(opts, list(region=region)))
  extract_nc_vars_by_polygon_coords_args <- list(polygon = polygon, req_coords = req_coords, 
                                                 time_var = "Time", x_var = "Lon", y_var = "Lat", 
                                                 is_longlat = FALSE, req_nc_coords = req_nc_coords)
  
  FUN_args = list(s3read_args = s3read_args, extract_nc_vars_by_polygon_coords_args = extract_nc_vars_by_polygon_coords_args)
  dt_years <- nc_files_grouped_dt[year %in% years]
  split_dts <- split(dt_years, by = "id")
  

  return(list(split_dts = split_dts, FUN_args = FUN_args))

}


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


main_function <- function(runner_path = "scripts/fmi_parallel_runner.R",
                          save_path = getwd(),
                          opts = NULL,
                          ...) {
  
  # Get params for function
  args <- list(...)
  
  # Call the get_fmi_data_from_allas with the provided params
  return_list <- do.call(get_fmi_data_from_allas, c(args, list(opts = opts)))
  
  assert_directory(save_path, access = "rw")
  
  # Save the return_list to a temporary file and unlink it on exit
  temp_file <- tempfile(fileext = ".rds")
  saveRDS(list(return_list = return_list, save_path = save_path, opts = opts), file = temp_file)
  on.exit(unlink(temp_file), add = TRUE)
  
  # Call the script that runs the parallel function using system()
  # system(paste("Rscript", runner_path, temp_file))
  processx::run("Rscript", c(runner_path, temp_file), echo = T)
  
}



test_main_function <- function(runner_path = "scripts/fmi_parallel_runner.R",
                          save_path = getwd(),
                          opts = NULL,
                          ...) {
  
  # Get params for function
  args <- list(...)
  
  print(paste0("args: ", args))
  
  return(save_path)
  
}

















