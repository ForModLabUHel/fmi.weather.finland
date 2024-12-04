# This script runs the parallel part of extracting the FMI data and saves the output. 
# The script reads a temporary file containing the required information to run. 
# The code is executed inside the environment that is loaded from the file.



local({
  # Get the temporary file path from the command-line arguments
  args <- commandArgs(trailingOnly = TRUE)
  temp_file <- args[1]
  
  # Load the saved data
  all_data <- readRDS(temp_file)
  env <- all_data$env
  
  with(env, {
    print(paste0("Loading libs..."))
    load_libraries(required_libs)
    
    # Get region
    region <- Sys.getenv("AWS_REGION")
    print(region)
    
    # Execute the parallel function
    var_dt <- execute_parallel(data)
    
    cat("\n")
    print(paste0("Unique coords:"))
    print(nrow(unique(var_dt[, .(x, y)])))
    cat("\n")
    
    save_path <- data$save_path
    req_coords_lookup_dt <- data$return_list$req_coords_lookup_dt
    format_to_prebas <- data$format_to_prebas
    
    print(paste0("Lookup table:"))
    print(req_coords_lookup_dt)
    cat("\n")
    
    timestamp <- generate_unique_filename("", "rdata")
    fmi_vars_filename <- paste0("fmi_vars_RAW", timestamp)
    lookup_filename <- paste0("climID_lookup", timestamp)
    
    if(format_to_prebas) {
      print(paste0("Transforming into Prebas format..."))
      var_dt <- transform_raw_fmi_to_prebas(var_dt, config, region) # To Prebas format
      print("Done")
      cat("\n")
      fmi_vars_filename <- paste0("fmi_vars_PREBAS", timestamp)
    }
    
    print(paste0("Result table:"))
    print(var_dt)
    cat("\n")
    
    save_fmi_files_with_print(save_path = save_path, 
                              objects = list(var_dt, req_coords_lookup_dt),
                              filenames = list(fmi_vars_filename, lookup_filename))
    
    
  })
})


















