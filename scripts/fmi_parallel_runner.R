

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
    
    # Execute the parallel function
    var_dt <- execute_parallel(data)
    
    cat("\n")
    print(paste0("Result table:"))
    print(var_dt)
    cat("\n")
    
    print(paste0("Unique coords:"))
    print(nrow(unique(var_dt[, .(x, y)])))
    cat("\n")
    
    save_path <- data$save_path
    req_coords_lookup_dt <- data$return_list$req_coords_lookup_dt
    
    print(req_coords_lookup_dt)
    
    timestamp <- generate_unique_filename("", "rdata")
    fmi_vars_filename <- paste0("fmi_vars", timestamp)
    lookup_filename <- paste0("climID_lookup", timestamp)
    
    save_fmi_files_with_print(save_path = save_path, 
                              objects = list(var_dt, req_coords_lookup_dt),
                              filenames = list(fmi_vars_filename, lookup_filename))
    
    
  })
})


















