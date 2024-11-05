# Script that loads an environment from a tempfile and executes a parallel script.


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
    
    save_path <- data$save_path 
    
    # Save with unique filename
    filename <- generate_unique_filename("fmi_vars", "rdata")
    full_path <- file.path(save_path, filename)
    
    print(paste0("Saving file ", full_path, "..."))
    save(var_dt, file = full_path)
    print("Done.")
  })
})



