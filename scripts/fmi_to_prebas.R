# This script is for transforming a data.table of FMI meteorogical data into
# the correct format for Prebas.

source("scripts/settings.R")
source(config$fmi_top_level_funs)


# Load functions and packages
setup_environment(sources, required_libs)


# Load unprocessed FMI data
load_path <- file.path(getwd(), "data", "rdata", "fmi_from_allas")
latest_file <- get_latest_filename_in_dir(load_path, "fmi_vars", full_name = F)
full_load_path <- file.path(load_path, latest_file)
fmi_dt <- load_rdata_file(full_load_path)

# Calculate photon flux density 
fmi_dt$par <- fmi_dt$Globrad*0.48*4.6/1000 

# Calculate VPD 
fmi_dt[, vpd := VPD_from_rh_tmin_tmax(Rh, Tmin, Tmax)] 

# Add co2
co2_path <- paste0("data/rdata/co2_mm_mol_1961_2024.rdata")
co2_dt <- load_rdata_file(co2_path)
co2_unique_years <- unique(co2_dt[between(year,range(year(fmi_dt$time))[1], range(year(fmi_dt$time))[2])])
fmi_dt[, c("year","month") := .(year(time), month(time))]
fmi_dt[co2_unique_years, on = .(year, month), co2 := i.monthly.average]

# Delete variables that are not used in PREBAS 
fmi_dt[, `:=`(year = NULL, month = NULL, Globrad = NULL, Rh = NULL)]

# Remove 29.2 from leap years
fmi_dt <- fmi_dt[!(day(fmi_dt$time) == 29 & month(fmi_dt$time) == 2), ]

# Rename columns
old_names <- colnames(fmi_dt)
new_names <- c("id", "time", "x", "y", "precip", "tair", "tmax", "tmin", "par", "vpd", "co2")
setnames(fmi_dt, old = old_names, new = new_names)

# Save
file_id <- extract_timestamp_from_filename(latest_file, prefix = "fmi_vars")
save_filename <- paste0("PREBAS_fmi_vars_", file_id, ".rdata")
save_path <- file.path(getwd(), "data", "rdata", save_filename)
save(fmi_dt, file = save_path)




