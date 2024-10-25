source("scripts/settings.R")
source("scripts/tempdir_test.R")

# Get the temporary file path from the command-line arguments
args <- commandArgs(trailingOnly = TRUE)
temp_file <- args[1]

# Load the saved data
data <- readRDS(temp_file)
save_path <- data$save_path

# Execute the parallel function
var_dt <- execute_parallel(data)


print(var_dt)



# Save with unique filename
filename <- generate_unique_filename("fmi_vars", "rdata")
full_path <- file.path(save_path, filename)

print(paste0("Saving file ", full_path, "..."))
# save(var_dt, file = full_path)
print("Done.")


















