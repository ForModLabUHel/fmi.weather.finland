

# Functions




#' Open NetCDF File or Use Existing ncdf4 Object
#'
#' This function opens a NetCDF file if a file path is provided, or uses an existing ncdf4 object.
#'
#' @param nc_input Either a string representing the path to the NetCDF file or an already opened netCDF object.
#' @return A list containing the ncdf4 object and a flag indicating if the file should be closed.
#' @import ncdf4
#' @import checkmate
#' @export
open_nc_file <- function(nc_input) {
  if (is.character(nc_input)) {
    assert_file_exists(nc_input)
    nc_ds <- nc_open(nc_input)
    return(list(nc_ds = nc_ds, should_close = TRUE))
  } else if (inherits(nc_input, "ncdf4")) {
    return(list(nc_ds = nc_input, should_close = FALSE))
  } else {
    stop("nc_input must be either a file path or an ncdf4 object")
  }
}


#' Open a NetCDF File
#'
#' This function opens a NetCDF file and returns a NetCDF object.
#'
#' @param netCdf_path The file path to the NetCDF file.
#'
#' @return A NetCDF object.
#' @examples
#' \dontrun{
#' nc <- open_netcdf("path_to_your_file.nc")
#' }
#' @export
open_netcdf <- function(netCdf_path) {
  tryCatch(nc_open(netCdf_path), error = function(e) stop("Error opening NetCDF file: ", e))
}


#' Get Dimensions from NetCDF File
#'
#' This function retrieves the longitude, latitude, and time dimensions from a NetCDF file.
#'
#' @param nc A NetCDF object from which to extract dimensions.
#' @param time_var The name of the time variable in the NetCDF file.
#' @param x_var Name of the x-dimension variable (e.g., longitude or easting) in the NetCDF file.
#' @param y_var Name of the y-dimension variable (e.g., latitude or northing) in the NetCDF file.
#' @param round_dec An integer indicating the number of decimal places to round the coordinates.
#' @param is_longlat A logical value indicating whether the coordinates are in longitude/latitude (TRUE) or in a projected coordinate system (FALSE).
#'
#' @return A list containing the rounded x, y, and time dimensions.
#' @examples
#' \dontrun{
#' nc <- nc_open("path_to_your_file.nc")
#' get_dimensions(nc, "time", "lon", "lat", 2)
#' }
#' @export
get_dimensions <- function(nc, time_var, x_var, y_var, round_dec, is_longlat = TRUE) {
  # Input validation
  checkmate::assert_class(nc, "ncdf4")
  checkmate::assert_string(time_var)
  checkmate::assert_string(x_var)
  checkmate::assert_string(y_var)
  checkmate::assert_number(round_dec, lower = 0)
  checkmate::assert_flag(is_longlat)
  
  dim_x <- round(ncvar_get(nc, varid = x_var), round_dec)
  dim_y <- round(ncvar_get(nc, varid = y_var), round_dec)
  
  if(is_longlat) {
    dim_x <- round(adjust_lon_vals(dim_x), round_dec)
    dim_y <- validate_lat_vals(dim_y)
  }
  
  dim_time <- ncvar_get(nc, time_var)
  t_units <- ncatt_get(nc, time_var, "units")
  date_origin <- ymd(strsplit(t_units$value, " ")[[1]][3])
  dim_time <- date_origin + dim_time
  
  return(list(dim_x = dim_x, dim_y = dim_y, dim_time = dim_time))
}

#' Adjust Longitude Values
#'
#' This function adjusts longitude values that are greater than 180 and less than -180.
#'
#' @param lon_vals A numeric vector of longitude values. 
#' Issues a warning if values are numeric but outside the range of -180 to 180.
#' @return A numeric vector with adjusted longitude values.
#' @examples
#' adjust_lon_vals(c(150, 190, 200, 170, 360))
#' @export
adjust_lon_vals <- function(lon_vals) {
  # Input validation
  assert_numeric_with_warning(lon_vals, -180, 180, "Longitude")
  
  # Adjust longitude values
  adjusted_lon_vals <-   ((lon_vals + 180) %% 360) - 180
  
  return(adjusted_lon_vals)
}


#' Validate Latitude Values
#'
#' This function validates that the input latitude values are numeric and within the range of -90 to 90 degrees.
#' Issues a warning if values are numeric but outside the correct range.
#'
#' @param lat_vals A numeric vector of latitude values.
#' @return The validated latitude values.
#' @examples
#' validate_lat_vals(c(45, -30, 90))
#' @export
validate_lat_vals <- function(lat_vals) {
  # Input validation
  assert_numeric_with_warning(lat_vals, -90, 90, "Latitude")
  
  return(lat_vals)
}

#' Assert Numeric with Warning
#'
#' This function checks if the provided numeric values are within the specified range.
#' If the values are outside the range, it issues a warning suggesting that the values
#' might be projected instead of in longlat format.
#'
#' @param x A numeric vector to be checked.
#' @param lower The lower bound of the acceptable range.
#' @param upper The upper bound of the acceptable range.
#' @param var_name A character string representing the name of the variable being checked.
#'
#' @return No return value, called for side effects.
#' @export
#'
#' @examples
#' lat_vals <- c(91, -45, 60)
#' lon_vals <- c(180, -370, 45)
#' assert_numeric_with_warning(lat_vals, -90, 90, "Latitude")
#' assert_numeric_with_warning(lon_vals, -360, 360, "Longitude")
assert_numeric_with_warning <- function(x, lower, upper, var_name) {
  if (checkmate::test_numeric(x, any.missing = FALSE)) {
    if (!checkmate::test_numeric(x, lower = lower, upper = upper)) {
      warning(sprintf("Warning: %s values might be projected instead of longlat. Please check the range.", var_name))
    }
  } else {
    checkmate::assert_numeric(x, any.missing = FALSE, lower = lower, upper = upper)
  }
}




#' Find Nearest Coordinates
#'
#' This function finds the nearest coordinates from a set of requested coordinates based on a given coordinate system.
#'
#' @param req_coords A matrix of requested coordinates.
#' @param dim_x A vector of x-dimension values (e.g., longitude or easting). Default is NULL
#' @param dim_y A vector of y-dimension values (e.g., latitude or northing). Default is NULL
#' @param nc_coords A matrix of x and y coordinates values (e.g., latitude or northing). Default is NULL
#' @param round_dec An integer indicating the number of decimal places to round the coordinates.
#' @param is_longlat A logical value indicating whether the coordinates are in longitude/latitude (TRUE) or in a projected coordinate system (FALSE).
#'
#' @return A matrix of the nearest coordinates rounded to the specified number of decimal places.
#' @examples
#' \dontrun{
#' req_coords <- matrix(c(500000, 4649776, 500100, 4649876), ncol = 2)
#' dim_lon <- seq(500000, 500200, by = 50)
#' dim_lat <- seq(4649776, 4649976, by = 100)
#' round_dec <- 2
#' find_nearest_coords(req_coords, dim_lon, dim_lat, round_dec, is_longlat = FALSE)
#' }
#' @export
find_nearest_coords <- function(req_coords, dim_x = NULL, dim_y = NULL, nc_coords = NULL, round_dec, is_longlat = TRUE) {
  
  # Input validations
  assert_numeric(dim_x, any.missing = FALSE, min.len = 1, null.ok = TRUE)
  assert_numeric(dim_y, any.missing = FALSE, min.len = 1, null.ok = TRUE)
  assert_matrix(req_coords, mode = "numeric", any.missing = FALSE, min.rows = 1, min.cols = 2)
  assert_matrix(nc_coords, mode = "numeric", any.missing = FALSE, min.rows = 1, min.cols = 2, null.ok = TRUE)
  assert_int(round_dec, lower = 0)
  assert_flag(is_longlat)
  
  
  if(is.null(nc_coords)) {
    assert_numeric(dim_x, any.missing = FALSE, min.len = 1, null.ok = FALSE)
    assert_numeric(dim_y, any.missing = FALSE, min.len = 1, null.ok = FALSE)
    nc_coords <- as.matrix(expand.grid(dim_x, dim_y))
  }
  
  
  ind <- apply(req_coords, 1, function(coord) which.min(spDistsN1(pt = coord, pts = nc_coords, longlat = is_longlat)))
  ind_coords <- round(nc_coords[ind, ], round_dec)
  return(matrix(ind_coords, ncol = 2))
}



#' Retrieve Variable Data from NetCDF File
#'
#' This function extracts specified variables from a NetCDF file based on given coordinates and time dimensions.
#'
#' @param nc A NetCDF object from which to extract data.
#' @param req_var A character vector of variable names to be extracted.
#' @param coord_idxs A data.table containing the indices of the coordinates.
#' @param dim_time A vector representing the time dimension.
#' @param id_vec A numeric vector of IDs corresponding to the coordinate indices.
#' @param req_nc_coords A matrix of required NetCDF coordinates (longitude and latitude).
#' @param is_longlat A logical value indicating whether the coordinates are in longitude/latitude (TRUE) or in a projected coordinate system (FALSE).
#'
#' @return A data.table containing the extracted data for the specified variables.
#' @examples
#' \dontrun{
#' nc <- nc_open("path_to_your_file.nc")
#' req_var <- c("temperature", "humidity")
#' coord_idxs <- data.table(lon_idxs = c(1, 2), lat_idxs = c(3, 4))
#' dim_time <- seq(as.Date("2000-01-01"), as.Date("2000-12-31"), by = "day")
#' id_vec <- c(1, 2)
#' req_nc_coords <- matrix(c(10, 20, 30, 40), ncol = 2)
#' get_variable_data(nc, req_var, coord_idxs, dim_time, id_vec, req_nc_coords)
#' }
#' @export
get_variable_data <- function(nc, req_var, coord_idxs, dim_time, id_vec, req_nc_coords, is_longlat = TRUE) {
  
  
  # Input validation
  coll <- makeAssertCollection() # Collection for error messages
  assert_class(nc, "ncdf4")
  assert_character(req_var, min.len = 1)
  assert_data_frame(coord_idxs, min.rows = 1, any.missing = F, all.missing = F, ncols = 2)
  x_idxs_assertion <- assert_numeric(coord_idxs$x_idxs, any.missing = F, all.missing = F, add = coll)
  y_idxs_assertion <- assert_numeric(coord_idxs$y_idxs, any.missing = F, all.missing = F, add = coll)
  assert_numeric(dim_time, min.len = 1)
  assert_numeric(id_vec, min.len = 1)
  assert_matrix(req_nc_coords, ncols = 2)
  
  if(!coll$isEmpty()) {
    coll$push(c("Possible cause: No matching values found for req_nc_coords.",
                "Possible fixes:", "1. Modify parameter round_dec to prevent rounding errors.",
                "2. Provide req_coords instead of req_nc_coords." ))
    
    reportAssertions(coll)
    
  }
  
  
  dt_all_vars <- data.table()  # Initialize an empty data.table
  
  # Loop through each requested variable
  for (var in req_var) {
    dt <- as.data.table(coord_idxs)  # Convert coordinate indices to data.table
    dt[, row := .I]  # Add a row index
    
    # Preallocate vectors for results
    id_vec <- id_vec
    x_idxs <- dt$x_idxs
    y_idxs <- dt$y_idxs
    n <- nrow(dt)
    tmp_list <- vector("list", n)
    
    
    # Read all required data in one go
    all_vars <- ncvar_get(nc, varid = var, start = c(1, 1, 1), count = c(-1, -1, -1))
    
    # Extract and transform data
    for (i in 1:n) {
      tmp_list[[i]] <- all_vars[x_idxs[i], y_idxs[i], ]
    }
    
    # Combine results into data.table
    dt <- data.table(
      id = rep(id_vec, each = length(dim_time)),
      time = rep(dim_time, times = n),
      lon = rep(req_nc_coords[, 1], each = length(dim_time)),
      lat = rep(req_nc_coords[, 2], each = length(dim_time)),
      var = unlist(tmp_list)
    )
    
    
    # dt <- rbindlist(dt_list)  # Combine the list of data.tables into one
    setnames(dt, "var", var)  # Rename the variable column to the variable name
    dt_all_vars <- if (ncol(dt_all_vars) == 0) dt else cbind(dt_all_vars, dt[, .SD, .SDcols = var])  # Combine with the main data.table
  }
  # Set coordinate names to x and y if is_longlat = FALSE
  if(!is_longlat) {
    x_val <- "x"
    y_val <- "y"
    setnames(dt_all_vars, old = c("lon", "lat"), new = c(x_val, y_val))
  }
  
  return(dt_all_vars)  # Return the combined data.table
}






#' Retrieve NetCDF Data by Nearest Coordinates
#'
#' This function extracts specified variables from a NetCDF file based on the nearest coordinates to the requested coordinates.
#'
#' @param nc_input Either a string representing the path to the NetCDF file or an already opened netCDF object.
#' @param req_coords A matrix of requested coordinates (longitude and latitude).
#' @param req_var A character vector of variable names to be extracted. If NULL, all variables will be extracted.
#' @param id_vec A vector of IDs corresponding to the coordinate indices. Defaults to NULL.
#' @param time_var The name of the time variable in the NetCDF file. Defaults to "time".
#' @param x_var Name of the x-dimension variable (e.g., longitude or easting) in the NetCDF file.
#' @param y_var Name of the y-dimension variable (e.g., latitude or northing) in the NetCDF file.
#' @param round_dec An integer indicating the number of decimal places to round the coordinates. Defaults to 3.
#' @param req_nc_coords A matrix of requested coordinates (longitude and latitude) that are known to be in the NetCDF file. 
#' If NULL (default), coordinates will be determined by finding the nearest neighbours of req_coords.
#' @param is_longlat A logical value indicating whether the coordinates are in longitude/latitude (TRUE) or in a projected coordinate system (FALSE).
#'
#' @return A data.table containing the extracted data for the specified variables.
#' @examples
#' \dontrun{
#' nc_input <- "path_to_your_file.nc"
#' req_coords <- matrix(c(10.5, 20.5, 30.5, 40.5), ncol = 2)
#' req_var <- c("temperature", "humidity")
#' get_netcdf_by_nearest_coords(nc_input, req_coords, req_var)
#' }
#' @export
get_netcdf_by_nearest_coords <- function(nc_input, req_coords, req_var = NULL, id_vec = NULL, time_var = "time",
                                         x_var = "longitude", y_var = "latitude", round_dec = 3, 
                                         req_nc_coords = NULL, is_longlat = TRUE) {
  
  # Open NetCDF file or use existing ncdf4 object
  nc_info <- open_nc_file(nc_input)
  nc_ds <- nc_info$nc_ds
  should_close <- nc_info$should_close
  
  # Ensure the NetCDF file is closed if it was opened by this function
  if (should_close) {
    on.exit(nc_close(nc_ds))
  }
  
  # Input validation
  assert_matrix(req_coords, min.rows = 1, min.cols = 2)
  assert_character(req_var, null.ok = TRUE)
  assert_numeric(id_vec, null.ok = TRUE)
  assert_character(time_var, len = 1)
  assert_character(x_var, len = 1)
  assert_character(y_var, len = 1)
  assert_number(round_dec, lower = 0)
  assert_matrix(req_nc_coords, null.ok = TRUE, min.rows = 1, min.cols = 2)
  assert_flag(is_longlat)
  
  # Check if the NetCDF file contains variables
  if (length(nc_ds$var) == 0) {
    stop("The NetCDF file does not contain any variables.")
  }
  
  # If req_var is not provided, get all variable names
  if (is.null(req_var)) {
    req_var <- names(nc_ds$var)
  }
  
  # If id_vec is not provided, create a default one
  if (is.null(id_vec)) id_vec <- 1:nrow(req_coords)
  
  # Get the dimensions (eg. longitude, latitude, time) from the NetCDF file
  dims <- get_dimensions(nc_ds, time_var, x_var, y_var, round_dec, is_longlat)
  
  # If req_nc_coords is not provided, find the nearest coordinates
  if (is.null(req_nc_coords)) {
    req_nc_coords <- find_nearest_coords(req_coords = req_coords, 
                                         dim_x = dims$dim_x, 
                                         dim_y = dims$dim_y, 
                                         nc_coords = NULL,
                                         round_dec = round_dec,
                                         is_longlat = is_longlat)
  }
  
  # Round to round_dec
  req_nc_coords <- round(req_nc_coords, round_dec)
  
  # Match the requested coordinates to the NetCDF dimensions
  x_idxs <- sapply(req_nc_coords[,1], function(x) which(unique(dims$dim_x) == x))
  y_idxs <- sapply(req_nc_coords[,2], function(x) which(unique(dims$dim_y) == x))
  coord_idxs <- cbind(x_idxs, y_idxs)
  
  # To data.table
  coord_idxs_dt <- as.data.table(coord_idxs)
  
  # Retrieve the variable data from the NetCDF file
  dt_all_vars <- get_variable_data(nc_ds, req_var, coord_idxs_dt, dims$dim_time, id_vec, req_nc_coords, is_longlat)
  
  # Return the data.table with all variables
  return(dt_all_vars)
}



