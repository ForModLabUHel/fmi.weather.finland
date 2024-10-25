library(yaml)
library(data.table)

config_file <- "config.yaml"
config <- yaml::read_yaml(config_file)

# Set envs
Sys.setenv(TZ = config$time_zone) # Set timezone
Sys.setenv(LANG = config$locale) # Set locale
Sys.setlocale("LC_CTYPE", config$locale) 

source(config$fmi_top_level_funs)

sources <- config$fmi_sources
required_libs <- config$required_libs
runner_path <- config$runner_path

setup_environment(sources, required_libs)



