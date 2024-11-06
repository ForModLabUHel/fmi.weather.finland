
# Functions for processing FMI data








##### ------------------- START NC FUNCTIONS ------------------- #####


#' Extract NetCDF Variables by Polygon Coordinates
#'
#' This function extracts variables from a NetCDF file based on specified polygon coordinates or provided coordinates.
#'
#' Required libraries:
#' \code{checkmate}, \code{sp}, \code{data.table}, \code{rgdal}
#'
#' @param nc_input Either a string representing the path to the NetCDF file or an already opened netCDF object.
#' @param polygon A \code{SpatialPolygons} object defining the area of interest. Defaults to NULL.
#' @param req_coords A matrix of coordinates to use directly. Defaults to NULL.
#' @param ... Additional arguments passed to \code{get_netcdf_by_nearest_coords}.
#'
#' @return A data.table containing the extracted variables.
#' @examples
#' \dontrun{
#' library(checkmate)
#' library(sp)
#' library(data.table)
#' library(rgdal)
#'
#' nc_input <- "path/to/netcdf/file.nc"
#' polygon <- readOGR("path/to/shapefile.shp")
#' result <- extract_nc_vars_by_polygon_coords(nc_input, polygon)
#' }
#' @export
extract_nc_vars_by_polygon_coords <- function(nc_input, polygon = NULL, 
                                              req_coords = NULL, lon_name = "Lon",
                                              lat_name = "Lat", ...) {
  
  # Open NetCDF file or use existing ncdf4 object
  nc_info <- open_nc_file(nc_input)
  nc_ds <- nc_info$nc_ds
  should_close <- nc_info$should_close
  
  # Ensure the NetCDF file is closed if it was opened by this function
  if (should_close) {
    on.exit(nc_close(nc_ds))
  }
  
  assert_list(list(...), null.ok = TRUE) # Ensure additional arguments are a list (if provided)
  assert_class(polygon, "SpatialPolygons", null.ok = TRUE) # Ensure the polygon is a SpatialPolygons object if provided
  assert_matrix(req_coords, null.ok = TRUE) # Ensure req_coords is a matrix if provided
  
  filename <- if (is.character(nc_input)) nc_input else "netCDF object"
  
  print(paste0("Processing ", filename, "..."))
  
  get_netcdf_by_nearest_coords_args <- list(...)
  
  # Extract coordinates within the specified polygon if req_coords is not provided
  if (is.null(req_coords)) {
    if (is.null(polygon)) {
      stop("Either 'polygon' or 'req_coords' must be provided.")
    }
    print(paste0("Getting coordinates within polygon..."))
    req_coords <- as.matrix(extr_coords_from_netCDF_within_plgn(nc_ds, polygon, lonName = lon_name, latName = lat_name))
    # Overwrite or add 'req_nc_coords' to the list
    get_netcdf_by_nearest_coords_args$req_nc_coords <- req_coords
    
  } else {
    print(paste0("Using provided coordinates..."))
  }
  
  
  # Prepare arguments for the get_netcdf_by_nearest_coords function
  get_netcdf_by_nearest_coords_args <- c(get_netcdf_by_nearest_coords_args,
                                         list(nc_input = nc_input,
                                              req_coords = req_coords))
  
  print(paste0("Getting variables..."))
  
  # Extract variables from the nc file
  nc_vars_dt <- do.call(get_netcdf_by_nearest_coords, get_netcdf_by_nearest_coords_args)
  
  print(paste0("Done."))
  
  # Return the extracted variables
  return(nc_vars_dt)
}


#' Extract NetCDF Variables by Coordinates
#'
#' This function extracts variables from a NetCDF file using the provided coordinates. It can handle both general coordinates (`req_coords`) and NetCDF-specific coordinates (`req_nc_coords`).
#'
#' @param nc_input A character string specifying the path to the NetCDF file or an existing `ncdf4` object.
#' @param req_coords A numeric matrix of requested coordinates. Default is `NULL`.
#' @param req_nc_coords A numeric matrix of requested NetCDF coordinates. Default is `NULL`.
#' @param lon_name A character string specifying the longitude variable name in the NetCDF file. Default is `"Lon"`.
#' @param lat_name A character string specifying the latitude variable name in the NetCDF file. Default is `"Lat"`.
#' @param ... Additional arguments passed to the `get_netcdf_by_nearest_coords` function.
#' @return A data.table containing the extracted variables from the NetCDF file.
#' @details 
#' This function performs the following steps:
#' \itemize{
#'   \item Opens the specified NetCDF file or uses an existing `ncdf4` object.
#'   \item Validates that either `req_coords` or `req_nc_coords` is provided.
#'   \item Uses `req_nc_coords` if provided; otherwise, it uses `req_coords`.
#'   \item Prepares the arguments for the `get_netcdf_by_nearest_coords` function.
#'   \item Extracts and returns the variables from the NetCDF file.
#' }
#' @examples
#' \dontrun{
#' library(ncdf4)
#' nc_path <- "path/to/netcdf_file.nc"
#' req_coords <- matrix(c(300000, 6800000, 301000, 6801000, 302000, 6802000), ncol = 2)
#' req_nc_coords <- matrix(c(300000, 6800000, 300000, 6800000, 300000, 6800000), ncol = 2)
#' nc_vars_dt <- extract_nc_vars_by_coords(nc_input = nc_path, req_coords = req_coords, lon_name = "lon", lat_name = "lat")
#' }
#' @importFrom ncdf4 nc_open nc_close
#' @export
extract_nc_vars_by_coords <- function(nc_input, req_coords = NULL, req_nc_coords = NULL, lon_name = "Lon", lat_name = "Lat", ...) {
  
  # Open NetCDF file or use existing ncdf4 object
  nc_info <- open_nc_file(nc_input)
  nc_ds <- nc_info$nc_ds
  should_close <- nc_info$should_close
  
  # Ensure the NetCDF file is closed if it was opened by this function
  if (should_close) {
    on.exit(nc_close(nc_ds))
  }
  
  # Validate inputs
  if (is.null(req_coords) && is.null(req_nc_coords)) {
    stop("Either 'req_coords' or 'req_nc_coords' must be provided.")
  }
  
  assert_matrix(req_coords, null.ok = TRUE) # Ensure req_coords is a matrix if provided
  assert_matrix(req_nc_coords, null.ok = TRUE) # Ensure req_nc_coords is a matrix if provided
  assert_list(list(...), null.ok = TRUE) # Ensure additional arguments are a list (if provided)
  
  filename <- if (is.character(nc_input)) nc_input else "netCDF object"
  
  print(paste0("Processing ", filename, "..."))
  
  # Determine which coordinates to use
  if (!is.null(req_nc_coords)) {
    req_coords <- req_nc_coords
  }
  
  # Prepare arguments for the get_netcdf_by_nearest_coords function
  get_netcdf_by_nearest_coords_args <- list(..., nc_input = nc_input, req_coords = req_coords, req_nc_coords = req_nc_coords)
  
  print(paste0("Getting variables..."))
  
  # Extract variables from the nc file
  nc_vars_dt <- do.call(get_netcdf_by_nearest_coords, get_netcdf_by_nearest_coords_args)
  
  print(paste0("Done."))
  
  # Return the extracted variables
  return(nc_vars_dt)
}



#' Extract Coordinates from netCDF File
#'
#' This function extracts coordinates from a netCDF file that fall within a specified polygon.
#'
#' @param nc_input Either a string representing the path to the netCDF file or an already opened netCDF object.
#' @param polygon_in A SpatialPolygons object representing the polygon within which coordinates are to be extracted.
#' @param lonName A string representing the name of the longitude variable in the netCDF file. Default is "Lon".
#' @param latName A string representing the name of the latitude variable in the netCDF file. Default is "Lat".
#' @return A data.table containing the coordinates that fall within the specified polygon.
#' @import ncdf4
#' @import data.table
#' @import sp
#' @import checkmate
#' @import raster
#' @export
#' @examples
#' \dontrun{
#' nc_fname <- "path/to/netcdf/file.nc"
#' polygon_in <- sp::SpatialPolygons(...)
#' coords <- extr_coords_from_netCDF_within_plgn(nc_fname, polygon_in)
#' }
extr_coords_from_netCDF_within_plgn <- function(nc_input, polygon_in, lonName = "Lon", latName = "Lat") {
  
  # Open NetCDF file or use existing ncdf4 object
  nc_info <- open_nc_file(nc_input)
  nc_ds <- nc_info$nc_ds
  should_close <- nc_info$should_close
  
  # Ensure the NetCDF file is closed if it was opened by this function
  if (should_close) {
    on.exit(nc_close(nc_ds))
  }
  
  assert_class(polygon_in, "SpatialPolygons")
  assert_string(lonName)
  assert_string(latName)
  
  dim_lon <- ncvar_get(nc_ds, lonName)
  dim_lat <- ncvar_get(nc_ds, latName)
  
  coords <- as.data.table(expand.grid(dim_lon, dim_lat))
  setnames(coords, c("x", "y"))
  coordinates(coords) <- ~x + y
  proj4string(coords) <- crs(polygon_in)
  coordSel <- coords[complete.cases(over(coords, polygon_in)), ]
  coordSel <- as.data.table(coordSel)
  
  return(coordSel)
}




#' Get Requested Nearest Coordinates
#'
#' This function finds the nearest coordinates for the requested coordinates based on reference coordinates.
#'
#' @param req_coords A matrix with 2 columns and at least one row specifying the requested coordinates.
#' @param reference_coords_dt A data.table with 2 columns and at least one row specifying the reference coordinates.
#' @param ... Additional arguments passed to find_nearest_coords.
#' @return A data.table of the nearest coordinates.
#' @import data.table
#' @import checkmate
#' @examples
#' reference_coords_dt <- data.table(x = runif(10), y = runif(10))
#' req_coords <- matrix(runif(4), ncol = 2)
#' get_req_nc_coords(req_coords, reference_coords_dt)
#'
get_req_nc_coords <- function(req_coords, reference_coords_dt, ...) {
  
  if(is.null(req_coords)) {
    return(NULL)
  }
  
  # Validate inputs
  assert_matrix(req_coords, ncols = 2, min.rows = 1)
  assert_data_table(reference_coords_dt, min.rows = 1)
  
  coords_x <- unique(reference_coords_dt[, 1][[1]])
  coords_y <- unique(reference_coords_dt[, 2][[1]])
  
  args <- c(list(req_coords = req_coords, dim_x = coords_x, dim_y = coords_y), ...)
  req_nc_coords <- do.call(find_nearest_coords, args)
  
  return(req_nc_coords)
}


##### ------------------- END NC FUNCTIONS ------------------- #####







##### ------------------- START PROCESS DATA FUNCTIONS ------------------- #####





#' Create Climate ID Lookup Data Table
#'
#' This function takes two matrices, converts them to data.tables, creates an ID column for the first table indicating the row number,
#' then cbinds the two tables and assigns a second ID column called climID that indicates the unique coordinates in the second data.table.
#'
#' @param matrix1 A numeric matrix.
#' @param matrix2 A numeric matrix. Must have the same number of rows as \code{matrix1}.
#' @return A data.table with an ID column from the first matrix and a climID column indicating unique coordinates in the second matrix.
#' @examples
#' \dontrun{
#' matrix1 <- matrix(c(300000, 6800000, 301000, 6801000, 302000, 6802000), ncol = 2)
#' matrix2 <- matrix(c(300000, 6800000, 301000, 6801000, 302000, 6802000), ncol = 2)
#' result <- create_clim_id_lookup_dt(matrix1, matrix2)
#' print(result)
#' }
#' @importFrom data.table as.data.table setnames
#' @importFrom checkmate assert_matrix assert_true
#' @export
create_clim_id_lookup_dt <- function(matrix1, matrix2) {
  
  
  # Input validation
  assert_matrix(matrix1, min.rows = 1)
  assert_matrix(matrix2, min.rows = 1)
  assert_true(nrow(matrix1) == nrow(matrix2))
  
  
  # Convert matrices to data.tables
  dt1 <- as.data.table(matrix1)
  dt2 <- as.data.table(matrix2)
  
  # Create id columns
  dt1[, id := .I]
  dt2[, id := .I]
  
  # Assign climID to indicate unique coordinates in dt2
  dt2[, climID := .GRP, by = .(V1, V2)]  # Assuming second data.table has two columns (V1 and V2)
  
  # Merge climID into dt1 using id
  result_dt <- merge(dt1, dt2[, .(id, climID)], by = "id", all.x = TRUE)
  
  return(result_dt)
}











#' Extract Coordinates within a Polygon with Resolution
#'
#' This function extracts coordinates from a reference data table that fall within a given polygon.
#' If no points are found within the polygon, the function returns the centroid of the polygon.
#'
#' @param polygon_in A `SpatialPolygons` object representing the polygon.
#' @param reference_dt A `data.table` with x and y coordinates as columns.
#' @param resolution A numeric value representing the resolution (in km) for the output.
#' @param ... Additional arguments (currently not used).
#' @return A matrix of coordinates that fall within the polygon, or the centroid of the polygon if no points are found.
#' @details 
#' This function performs the following steps:
#' \itemize{
#'   \item Checks if the `polygon_in` is NULL. If so, it returns NULL.
#'   \item Sets the coordinates and projection string for the `reference_dt`.
#'   \item Identifies points within the polygon using the `over` function.
#'   \item If no points are found, it calculates and returns the centroid of the polygon.
#' }
#' @examples
#' \dontrun{
#' library(data.table)
#' library(sp)
#' library(checkmate)
#' library(sf)
#' 
#' # Create example data
#' reference_dt <- data.table(x = c(300000, 301000, 302000), y = c(6800000, 6801000, 6802000))
#' coords <- cbind(lon = c(300000, 305000, 305000, 300000), lat = c(6800000, 6800000, 6805000, 6805000))
#' polygon_in <- SpatialPolygons(list(Polygons(list(Polygon(coords)), "1")))
#' 
#' # Use the function
#' coords_matrix <- extract_polygon_coords_with_res(polygon_in, reference_dt, 5)
#' print(coords_matrix)
#' }
#' @importFrom data.table as.data.table setnames
#' @importFrom sp coordinates proj4string over
#' @importFrom sf st_as_sf st_centroid st_coordinates
#' @importFrom checkmate assert_class assert_data_table assert_numeric
#' @export
extract_polygon_coords_with_res <- function(polygon_in, reference_dt, resolution, ...) {
  # Input validation
  assert_class(polygon_in, classes = "SpatialPolygons", null.ok = TRUE)
  assert_data_table(reference_dt, min.rows = 1, null.ok = F)
  assert_names(names(reference_dt), must.include = c("x", "y"))
  assert_numeric(resolution, lower = 0)
  
  if (is.null(polygon_in)) {
    return(NULL)
  }
  
  coordinates(reference_dt) <- ~x + y
  proj4string(reference_dt) <- crs(polygon_in)
  coordSel <- reference_dt[complete.cases(over(reference_dt, polygon_in)), ]
  coordSel <- as.data.table(coordSel)
  
  char_res <- paste0(resolution, "km-by-", resolution, "km")
  print(paste0("Found ", nrow(coordSel), " point(s) within polygon at ", char_res, "."))
  if (nrow(coordSel) == 0) {
    print("Using polygon centroid.")
    coordSel <- as.data.table(st_coordinates(st_centroid(st_as_sf(polygon_in)))) # Get polygon centroid as data.table
    setnames(coordSel, new = c("x", "y"))
  }
  coords_mat <- as.matrix(coordSel[, .(x, y)])
  return(coords_mat)
}




#' Process and Join Data Tables
#'
#' This function processes a data.table by applying a specified function to a variable and then joins the results.
#'
#' @param dt A data.table object.
#' @param fun A function to apply to the variable specified by `process_var_name`.
#' @param fun_args A list of additional arguments to pass to `fun`.
#' @param process_var_name A character string specifying the name of the variable to process.
#' @param join_by_vec A character vector specifying the columns to join by.
#'
#' @return A data.table resulting from the processing and joining operations.
#' @export
#'
#' @examples
#' library(data.table)
#' dt <- data.table(id = 1:3, value = list(1:2, 3:4, 5:6))
#' FUN <- function(x, add) { data.table(result = x + add) }
#' FUN_args <- list(add = 10)
#' process_from_grouped_dt_and_join(dt, FUN, FUN_args, "value", "result")
process_from_grouped_dt_and_join <- function(dt, FUN, FUN_args, process_var_name, join_by_vec) {
  
  # Input validations using checkmate
  assert_data_table(dt)
  assert_function(FUN)
  assert_list(FUN_args)
  assert_string(process_var_name)
  assert_character(join_by_vec)
  
  result_dt <- dt[, {
    processed_list <- lapply(get(process_var_name), function(var) {
      args_list <- c(list(var), FUN_args)
      do.call(FUN, args_list)
    })
    Reduce(function(x, y) full_join(x, y, by = join_by_vec), processed_list)
  }]
  
  return(result_dt)
}




##### ------------------- END PROCESS DATA FUNCTIONS ------------------- #####







##### ------------------- START S3 FUNCTIONS ------------------- #####




#' Get Lookup Data Table with Resolution from S3 Bucket
#'
#' This function loads a lookup coordinates .rdata file from an S3 bucket and returns a filtered data table based on the resolution.
#'
#' @param resolution A numeric or character specifying the resolution.
#' @param ... Additional arguments passed to s3read_using.
#' @return A data.table filtered by the specified resolution.
#' @import data.table
#' @import aws.s3
#' @import checkmate
#' @examples
#' get_lookup_dt_with_res_from_bucket("high", bucket = "my-bucket", object = "lookup.rdata")
#'
get_lookup_dt_with_res_from_bucket <- function(resolution, ...) {
  # Validate inputs
  assert_numeric(resolution, null.ok = TRUE)
  
  # Args for s3read
  s3read_args <- list(...)
  
  # Load the lookup coordinates .rdata file from bucket
  fmi_lookup_dt <- do.call(s3read_using, s3read_args)
  
  # Determine resolution column
  res_col <- str_c("res", resolution, sep = "_")
  
  # Check if the resolution column exists in the data table
  assert_true(res_col %in% colnames(fmi_lookup_dt))
  
  # Return filtered table
  return(fmi_lookup_dt[get(res_col)])
}





#' Get NetCDF Variables from S3 Bucket
#'
#' This function reads a NetCDF file from an S3 bucket, extracts variables by polygon coordinates, and returns the results.
#'
#' @param object A string representing the path to the NetCDF file in the S3 bucket.
#' @param ... Additional arguments passed to custom_s3read_using and extract_nc_vars_by_polygon_coords.
#' @return The result of the extract_nc_vars_by_polygon_coords function.
#' @import ncdf4
#' @import checkmate
#' @export
get_nc_vars_from_bucket <- function(object, ...) {
  
  # Input validations
  assert_character(object, len = 1)
  
  print(paste0("Object: ", object))
  
  args <- list(...)
  
  assert_list(args$s3read_args, null.ok = TRUE)
  assert_list(args$extract_nc_vars_by_coords_args, null.ok = TRUE)
  
  # Extract args
  s3read_args <- args$s3read_args
  extract_nc_vars_by_coords_args <- args$extract_nc_vars_by_coords_args
  
  # Open nc from bucket
  nc <- do.call(s3read_using, c(list(object = object), s3read_args))
  on.exit(nc_close(nc))
  
  # Get vars
  do.call(extract_nc_vars_by_coords, c(list(nc_input = nc), extract_nc_vars_by_coords_args))
}




##### ------------------- END S3 FUNCTIONS ------------------- #####








##### ------------------- START UTILITY FUNCTIONS ------------------- #####


# Helper Function to Handle Coordinates Precedence
handle_process_data_input_coords <- function(polygon, req_coords, req_nc_coords, filtered_fmi_lookup_dt, resolution, round_dec) {
  if (!is.null(req_nc_coords)) {
    req_coords <- req_nc_coords
    colnames(req_nc_coords) <- NULL
  } else if (!is.null(req_coords)) {
    req_nc_coords <- get_req_nc_coords(req_coords, reference_coords_dt = filtered_fmi_lookup_dt, round_dec = round_dec, is_longlat = FALSE)
  } else if (!is.null(polygon)) {
    req_coords <- extract_polygon_coords_with_res(polygon = polygon, reference_dt = filtered_fmi_lookup_dt, resolution = resolution)
    req_nc_coords <- get_req_nc_coords(req_coords, reference_coords_dt = filtered_fmi_lookup_dt, round_dec = round_dec, is_longlat = FALSE)
  }
  req_coords_lookup_dt <- create_clim_id_lookup_dt(req_coords, req_nc_coords)
  list(req_coords = req_coords, req_nc_coords = unique(req_nc_coords), req_coords_lookup_dt = req_coords_lookup_dt)
}

# Helper Function to Print Messages
print_process_data_messages <- function(years, resolution) {
  print(paste0("Running process_data..."))
  cat("\n")
  print(paste0("Year(s): "))
  print(paste0(years))
  cat("\n")
  char_res <- paste0(resolution, "km-by-", resolution, "km")
  print(paste0("Resolution is ", char_res, "."))
  cat("\n")
}



# Helper function to handle coordinates precedence
handle_coords_input_precedence <- function(polygon, req_coords, req_nc_coords) {
  if (!is.null(req_nc_coords)) {
    return(list(polygon = NULL, req_coords = NULL, req_nc_coords = req_nc_coords))
  } else if (!is.null(req_coords)) {
    return(list(polygon = NULL, req_coords = req_coords, req_nc_coords = NULL))
  } else {
    return(list(polygon = polygon, req_coords = req_coords, req_nc_coords = req_nc_coords))
  }
}

# Helper function to save files with print
save_fmi_files_with_print <- function(save_path, objects, filenames) {
  invisible(lapply(seq_along(objects), function(i) {
    full_path <- file.path(save_path, filenames[i])
    object <- objects[i]
    
    print(paste0("Saving ", full_path, "..."))
    # save(object, file = full_path)
    print("Done.")
  }))
}


#' Calculate Vapor Pressure Deficit (VPD) from Relative Humidity, Minimum and Maximum Temperature 
#' 
#' This function calculates the Vapor Pressure Deficit (VPD) using relative humidity (RH), minimum temperature (Tmin), and maximum temperature (Tmax). 
#' Allen, RG, Pereira, LS, Raes, D., & Smith, M. (1998). Crop evapotranspiration-Guidelines for computing crop water requirements-FAO Irrigation and drainage paper 56. FAO, Rome , 300 (9), D05109. 
#' 
#' @param RH Relative Humidity in percentage (%) 
#' @param Tmin Minimum Temperature in degrees Celsius (??C) 
#' @param Tmax Maximum Temperature in degrees Celsius (??C) 
#' @return VPD Vapor Pressure Deficit in kilopascals (kPa) 
#' @examples 
#' VPD_from_rh_tmin_tmax(50, 10, 20) 
#' @export 
VPD_from_rh_tmin_tmax <- function(RH, Tmin, Tmax) { 
  
  # Checkmate tests 
  checkmate::assert_numeric(RH, lower = 0, upper = 100, any.missing = FALSE) 
  checkmate::assert_numeric(Tmin, any.missing = FALSE) 
  checkmate::assert_numeric(Tmax, any.missing = FALSE) 
  
  # Tmin , Tmax in degrees Celsius 
  # RH in percentage 
  SVPmax <- 0.6108 * exp(17.27 * Tmax / (Tmax + 237.3)) 
  SVPmin <- 0.6108 * exp(17.27 * Tmin / (Tmin + 237.3)) 
  SVP <- (SVPmin + SVPmax) / 2 # saturation vapor pressure 
  AVP <- SVP * (RH / 100) # actual vapor pressure 
  VPD <- SVP - AVP # in kPa 
  
  return(VPD) 
}



#' Load Required Libraries
#'
#' This function loads the necessary libraries, installing them if they are not already installed.
#'
#' @param pkgs A character vector of package names to load. Defaults to an empty vector.
#' @return NULL
#' @importFrom utils install.packages
#' @examples
#' load_libraries(c("dplyr", "ggplot2"))
#' 
load_libraries <- function(pkgs = c(), ...) {
  if(is.null(pkgs)) {
    return(pkgs)
  }
  
  if(class(pkgs) != "character") {
    stop("pkgs must be a character vector.")
  }
  
  for (pkg in pkgs) {
    if (!suppressMessages(require(pkg, character.only = TRUE))){
      install.packages(pkg, dependencies = TRUE)
      library(pkg, character.only = TRUE)
    }
  }
}



#' Execute Parallel Processing
#'
#' This function runs parallel processing on the data and combines the results.
#'
#' @param data A list containing the `return_list`, `save_path`, and `opts`.
#' @return A data table containing the combined results of the parallel processing.
#' @import data.table parallel checkmate
#' @export
execute_parallel <- function(data) {
  # Input validation
  checkmate::assert_list(data, any.missing = T, min.len = 3)
  
  return_list <- data$return_list
  opts <- data$opts
  
  # Run in parallel
  var_dts <- get_nc_vars_parallel(return_list, opts)
  
  # Combine results
  var_dt <- data.table::rbindlist(var_dts, use.names = TRUE, fill = TRUE)
  
  return(var_dt)
}



##### ------------------- END UTILITY FUNCTIONS ------------------- #####







