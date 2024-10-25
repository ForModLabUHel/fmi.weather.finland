
# Functions for processing files




#' Load R Data File
#'
#' This function loads an R data file into a new environment and returns the object.
#'
#' @param rdata_file_path The path to the R data file to be loaded.
#'
#' @return Returns the object loaded from the rdata_file_path.
#'
#' @examples
#' # Assuming 'data.RData' contains an object named 'my_data'
#' my_data <- load_rdata_file('data.RData')
#'
#' @export
load_rdata_file <- function(rdata_file_path) {
  assertFileExists(rdata_file_path, access = "r")
  temp_env <- new.env()
  obj <- get(load(rdata_file_path), temp_env)
  return(obj)
}




#' Create a Grouped Data Table from Filenames
#'
#' This function takes a vector of filenames and splits them into a data table based on specified column names and grouping variables.
#'
#' @param filenames A character vector of filenames.
#' @param column_names A character vector of column names to assign to the split components of the filenames. The last column name will usually be the file extension.
#' @param group_vars A character vector of column names to group by.
#' @param sep A character string specifying the separator pattern used to split the filenames. Default is "[_]".
#' @param base_path An optional character string specifying the base path to prepend to the filenames.
#' 
#' @return A data.table with the split components of the filenames, an `id` column for grouping, and a `filename` column with the full path if `base_path` is provided.
#' @export
#'
#' @examples
#' filenames <- c("sample_1.txt", "sample_2.txt")
#' column_names <- c("sample", "number")
#' group_vars <- c("sample")
#' get_grouped_dt_from_filenames(filenames, column_names, group_vars)
#'
#' # Example with base_path
#' get_grouped_dt_from_filenames(filenames, column_names, group_vars, base_path = "/path/to/files")
#'
#' # Edge case: Different separator
#' filenames <- c("sample-1.txt", "sample-2.txt")
#' get_grouped_dt_from_filenames(filenames, column_names, group_vars, sep = "[-]")
#'
#' # Edge case: No filenames
#' get_grouped_dt_from_filenames(character(0), column_names, group_vars)
#' 
#' @import data.table
#' @import stringr
#' @import tools
#' @import checkmate
get_grouped_dt_from_filenames <- function(filenames, column_names, group_vars, sep = "[_]", base_path = NULL) {
  # Validate inputs
  assert_character(filenames, min.len = 1)  # Ensure filenames is a non-empty character vector
  assert_character(column_names, min.len = 1)  # Ensure column_names is a non-empty character vector
  assert_character(group_vars, min.len = 1)  # Ensure group_vars is a non-empty character vector
  assert_string(sep)  # Ensure sep is a single string
  assert_string(base_path, null.ok = TRUE)  # Ensure base_path is a single string or NULL
  
  # Remove file extensions using file_path_sans_ext from tools
  filenames_no_ext <- file_path_sans_ext(filenames)
  
  # Split filenames into a data table based on the separator
  dt <- as.data.table(str_split_fixed(filenames_no_ext, sep, length(column_names)))
  colnames(dt) <- column_names  # Assign column names to the data table
  
  # Create a grouping ID based on the specified grouping variables
  dt[, id := .GRP, by = group_vars]
  
  # Prepend base_path to filenames if base_path is provided
  if (!is.null(base_path)) {
    filenames <- file.path(base_path, filenames)
  }
  
  # Add the filenames (with base_path if provided) to the data table
  dt[, filename := filenames]
  
  return(dt)  # Return the resulting data table
}




#' Generate a Unique Filename
#'
#' Creates a unique filename based on the current timestamp.
#'
#' @param prefix A string to prefix the filename. Defaults to "file".
#' @param extension The file extension. Defaults to "rdata".
#' @return A string containing the generated unique filename.
#' @import checkmate
#' @examples
#' generate_unique_filename()
generate_unique_filename <- function(prefix = "file", extension = "rdata") {
  checkmate::assert_string(prefix)
  checkmate::assert_string(extension)
  
  timestamp <- format(Sys.time(), "%Y%m%d%H%M%S")
  filename <- paste0(prefix, "_", timestamp, ".", extension)
  return(filename)
}

#' Get the Latest Filename
#'
#' Finds the latest file in the specified directory with a given prefix and extension based.
#'
#' @param directory The directory to search in. Defaults to the current working directory.
#' @param prefix A string to prefix the filename. Defaults to "file".
#' @param extension The file extension. Defaults to "rdata".
#' @param full_name Logical, if TRUE returns the full file path, otherwise just the filename. Defaults to TRUE.
#' @return A string containing the latest filename.
#' @import checkmate
#' @examples
#' latest_filename()
get_latest_filename_in_dir <- function(directory = getwd(), prefix = "file", extension = "rdata", full_name = TRUE) {
  checkmate::assert_directory_exists(directory)
  checkmate::assert_string(prefix)
  checkmate::assert_string(extension)
  checkmate::assert_flag(full_name)
  
  files <- list.files(directory, pattern = paste0("^", prefix, "_\\d{14}\\.", extension), full.names = full_name)
  latest_file <- tail(sort(files), 1)
  return(latest_file)
}

#' Extract Timestamp from Filename
#'
#' Extracts the timestamp from a filename with a given prefix and extension.
#'
#' @param filename The filename to extract the timestamp from.
#' @param prefix A string to prefix the filename. Defaults to "file".
#' @param extension The file extension. Defaults to "rdata".
#' @return A string containing the extracted timestamp.
#' @import checkmate
#' @examples
#' extract_timestamp_from_filename("file_20210930123045.rdata")
extract_timestamp_from_filename <- function(filename, prefix = "file", extension = "rdata") {
  checkmate::assert_file_exists(filename)
  checkmate::assert_string(prefix)
  checkmate::assert_string(extension)
  
  pattern <- paste0(prefix, "_(\\d{8,14})\\.", extension)
  matches <- regmatches(filename, regexec(pattern, filename))
  timestamp <- matches[[1]][2]
  return(timestamp)
}
















