# Script to generate all download links for fmi data into a txt file. 
# The file can then be read by r-clone in a loop to download data directly into 
# an Allas bucket.

setwd("/scratch/project_2000994/PREBASruns/finRuns/Rsrc/samuel/fmi")

# Define variables
base_url <- "http://fmi-gridded-obs-daily-1km.s3-website-eu-west-1.amazonaws.com/Netcdf"
var_names <- c("Tday", "Tmin", "Tmax", "Globrad", "Rh", "RRday")
vars <- tolower(var_names)
years <- c(1961:2024)


# Open a connection to the file
file_conn <- file("urls.txt", open = "wt")

# Write all urls to file
invisible(lapply(seq_along(1:length(var_names)), function(i) {
    lapply(years, function(year) {
      file_name <- paste0(vars[[i]], "_", year, ".nc")
      url <- file.path(base_url, var_names[[i]], file_name)
      writeLines(url, file_conn)
    })
}))

# Close the connection to the file
close(file_conn)


















