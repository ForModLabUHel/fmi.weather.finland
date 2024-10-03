

# Functions



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
    dim_x <- adjust_lon_vals(dim_x)
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
#' This function adjusts longitude values that are greater than 180 by subtracting 360.
#'
#' @param lon_vals A numeric vector of longitude values.
#' @return A numeric vector with adjusted longitude values.
#' @examples
#' adjust_lon_vals(c(150, 190, 200, 170, 360))
#' @export
adjust_lon_vals <- function(lon_vals) {
  # Input validation
  checkmate::assert_numeric(lon_vals, any.missing = FALSE, lower = -360, upper = 360)
  
  # Adjust longitude values
  adjusted_lon_vals <- ifelse(lon_vals > 180, lon_vals - 360, lon_vals)
  
  return(adjusted_lon_vals)
}


#' Validate Latitude Values
#'
#' This function validates that the input latitude values are numeric and within the range of -90 to 90 degrees.
#'
#' @param lat_vals A numeric vector of latitude values.
#' @return The validated latitude values.
#' @examples
#' validate_lat_vals(c(45, -30, 90))
#' @export
validate_lat_vals <- function(lat_vals) {
  # Input validation
  checkmate::assert_numeric(lat_vals, any.missing = FALSE, lower = -90, upper = 90)
  
  return(lat_vals)
}




#' Find Nearest Coordinates
#'
#' This function finds the nearest coordinates from a set of requested coordinates based on a given coordinate system.
#'
#' @param req_coords A matrix of requested coordinates.
#' @param dim_x A vector of x-dimension values (e.g., longitude or easting).
#' @param dim_y A vector of y-dimension values (e.g., latitude or northing).
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
find_nearest_coords <- function(req_coords, dim_x, dim_y, round_dec, is_longlat = TRUE) {
  
  # Input validations
  assert_matrix(req_coords, mode = "numeric", any.missing = FALSE, min.rows = 1, min.cols = 2)
  assert_numeric(dim_x, any.missing = FALSE, min.len = 1)
  assert_numeric(dim_y, any.missing = FALSE, min.len = 1)
  assert_int(round_dec, lower = 0)
  assert_flag(is_longlat)
  
  nc_coords <- as.matrix(expand.grid(dim_x, dim_y))
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
get_variable_data <- function(nc, req_var, coord_idxs, dim_time, id_vec, req_nc_coords) {
  
  # Input validation
  assert_class(nc, "ncdf4")
  assert_character(req_var, min.len = 1)
  assert_data_frame(coord_idxs, min.rows = 1)
  assert_numeric(dim_time, min.len = 1)
  assert_numeric(id_vec, min.len = 1)
  assert_matrix(req_nc_coords, ncols = 2)
  
  dt_all_vars <- data.table()  # Initialize an empty data.table
  
  # Loop through each requested variable
  for (var in req_var) {
    dt <- as.data.table(coord_idxs)  # Convert coordinate indices to data.table
    dt[, row := .I]  # Add a row index
    
    # Extract data for each coordinate index
    dt_list <- lapply(1:nrow(dt), function(i) {
      tmp <- ncvar_get(nc, varid = var, start = c(dt$lon_idxs[i], dt$lat_idxs[i], 1), count = c(1, 1, -1))
      data.table(id = id_vec[i], time = dim_time, x = req_nc_coords[i, 1],
                 y = req_nc_coords[i, 2], var = tmp)
    })
    
    dt <- rbindlist(dt_list)  # Combine the list of data.tables into one
    setnames(dt, "var", var)  # Rename the variable column to the variable name
    dt_all_vars <- if (ncol(dt_all_vars) == 0) dt else cbind(dt_all_vars, dt[, .SD, .SDcols = var])  # Combine with the main data.table
  }
  
  return(dt_all_vars)  # Return the combined data.table
}



#' Retrieve NetCDF Data by Nearest Coordinates
#'
#' This function extracts specified variables from a NetCDF file based on the nearest coordinates to the requested coordinates.
#'
#' @param netCdf_path The file path to the NetCDF file.
#' @param req_coords A matrix of requested coordinates (longitude and latitude).
#' @param req_var A character vector of variable names to be extracted.
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
#' netCdf_path <- "path_to_your_file.nc"
#' req_coords <- matrix(c(10.5, 20.5, 30.5, 40.5), ncol = 2)
#' req_var <- c("temperature", "humidity")
#' get_netcdf_by_nearest_coords(netCdf_path, req_coords, req_var)
#' }
#' @export
get_netcdf_by_nearest_coords <- function(netCdf_path, req_coords, req_var, id_vec = NULL, time_var = "time",
                                         x_var = "longitude", y_var = "latitude", round_dec = 3, 
                                         req_nc_coords = NULL, is_longlat = TRUE) {
  
  # Input validation
  assert_file_exists(netCdf_path)
  assert_matrix(req_coords, min.rows = 1, min.cols = 2)
  assert_character(req_var, len = 1)
  assert_numeric(id_vec, null.ok = TRUE)
  assert_character(time_var, len = 1)
  assert_character(x_var, len = 1)
  assert_character(y_var, len = 1)
  assert_number(round_dec, lower = 0)
  assert_matrix(req_nc_coords, null.ok = TRUE, min.rows = 1, min.cols = 2)
  assert_flag(is_longlat)
  
  
  # Open the NetCDF file
  nc <- open_netcdf(netCdf_path)
  
  # If id_vec is not provided, create a default one
  if (is.null(id_vec)) id_vec <- 1:nrow(req_coords)
  
  # Get the dimensions (eg. longitude, latitude, time) from the NetCDF file
  dims <- get_dimensions(nc, time_var, x_var, y_var, round_dec,is_longlat)
  
  # If req_nc_coords is not provided, find the nearest coordinates
  if (is.null(req_nc_coords)) {
    req_nc_coords <- find_nearest_coords(req_coords, dims$dim_x, dims$dim_y, round_dec,is_longlat)
  }
  
  # Match the requested coordinates to the NetCDF dimensions
  lon_idxs <- sapply(req_nc_coords[,1], function(x) which(unique(dims$dim_x) == x))
  lat_idxs <- sapply(req_nc_coords[,2], function(x) which(unique(dims$dim_y) == x))
  coord_idxs <- cbind(lon_idxs, lat_idxs)
  
  # To data.table
  coord_idxs_dt <- as.data.table(coord_idxs)
  
  # Retrieve the variable data from the NetCDF file
  dt_all_vars <- get_variable_data(nc, req_var, coord_idxs_dt, dims$dim_time, id_vec, req_nc_coords)
  
  # Close the NetCDF file
  nc_close(nc)
  
  # Return the data.table with all variables
  return(dt_all_vars)
}





#' Extract Coordinates from netCDF File
#'
#' This function extracts coordinates from a netCDF file that fall within a specified polygon.
#'
#' @param nc_fname A string representing the path to the netCDF file.
#' @param polygon_in A SpatialPolygons object representing the polygon within which coordinates are to be extracted.
#' @param lonName A string representing the name of the longitude variable in the netCDF file. Default is "Lon".
#' @param latName A string representing the name of the latitude variable in the netCDF file. Default is "Lat".
#' @return A data.table containing the coordinates that fall within the specified polygon.
#' @import ncdf4
#' @import data.table
#' @import sp
#' @import checkmate
#' @export
#' @examples
#' \dontrun{
#' nc_fname <- "path/to/netcdf/file.nc"
#' polygon_in <- sp::SpatialPolygons(...)
#' coords <- extr_coords_fromnetCDF(nc_fname, polygon_in)
#' }
extr_coords_from_netCDF_within_plgn <- function(nc_fname, polygon_in, lonName = "Lon", latName = "Lat") {
  # Input validation
  assert_file_exists(nc_fname)
  assert_class(polygon_in, "SpatialPolygons")
  assert_string(lonName)
  assert_string(latName)
  
  nc_ds <- nc_open(nc_fname)
  
  dim_lon <- ncvar_get(nc_ds, lonName)
  dim_lat <- ncvar_get(nc_ds, latName)
  
  coords <- as.data.table(expand.grid(dim_lon, dim_lat))
  setnames(coords, c("x", "y"))
  coordinates(coords) <- ~x + y
  proj4string(coords) <- crs(polygon_in)
  coordSel <- coords[complete.cases(over(coords, polygon_in)), ]
  coordSel <- as.data.table(coordSel)
  
  nc_close(nc_ds)
  
  return(coordSel)
}



#' Extract Weather Data from FMI
#'
#' This function extracts weather data from netCDF files within a specified polygon.
#'
#' @param pathdata Character. Path to the directory containing netCDF files.
#' @param years Integer vector. Years for which data is to be extracted.
#' @param polygon SpatialPolygons. Polygon within which data is to be extracted.
#' @param myCoords Matrix. Matrix of coordinates.
#' @param vars Character vector. Variables to be extracted (default: c("tday", "tmin", "tmax", "globrad", "rh", "rrday")).
#' @param varNames Character vector. Corresponding variable names in the netCDF files (default: c("Tday", "Tmin", "Tmax", "Globrad", "Rh", "RRday")).
#' @param id_vec A vector of IDs corresponding to the coordinate indices. Defaults to NULL.
#' @param time_var The name of the time variable in the NetCDF file. Defaults to "time".
#' @param x_var Name of the x-dimension variable (e.g., longitude or easting) in the NetCDF file.
#' @param y_var Name of the y-dimension variable (e.g., latitude or northing) in the NetCDF file.
#' @param round_dec An integer indicating the number of decimal places to round the coordinates. Defaults to 3.
#' @param req_nc_coords A matrix of requested coordinates (longitude and latitude) that are known to be in the NetCDF file. 
#' If NULL (default), coordinates will be determined by finding the nearest neighbours of req_coords.
#' @param is_longlat A logical value indicating whether the coordinates are in longitude/latitude (TRUE) or in a projected coordinate system (FALSE).
#' @param remove29_02 Logical. Whether the 29/02 should be removed (default: TRUE).
#' @return A data frame containing the extracted weather data.
#' @export
#' @examples
#' \dontrun{
#' extract_weather_from_FMI("path/to/data", 2020:2021, my_polygon)
#' }
#' Extract Weather Data from FMI
#'
#' This function extracts weather data from netCDF files within a specified polygon.
#'
#' @param pathdata Character. Path to the directory containing netCDF files.
#' @param years Integer vector. Years for which data is to be extracted.
#' @param polygon SpatialPolygons. Polygon within which data is to be extracted. Defaults to NULL.
#' @param myCoords Matrix. Coordinates to be used if polygon is not provided. Defaults to NULL.
#' @param vars Character vector. Variables to be extracted (default: c("tday", "tmin", "tmax", "globrad", "rh", "rrday")).
#' @param varNames Character vector. Corresponding variable names in the netCDF files (default: c("Tday", "Tmin", "Tmax", "Globrad", "Rh", "RRday")).
#' @param id_vec A vector of IDs corresponding to the coordinate indices. Defaults to NULL.
#' @param time_var The name of the time variable in the NetCDF file. Defaults to "Time".
#' @param x_var Name of the x-dimension variable (e.g., longitude or easting) in the NetCDF file.
#' @param y_var Name of the y-dimension variable (e.g., latitude or northing) in the NetCDF file.
#' @param round_dec An integer indicating the number of decimal places to round the coordinates. Defaults to 3.
#' @param req_nc_coords A matrix of requested coordinates (longitude and latitude) that are known to be in the NetCDF file. 
#' If NULL (default), coordinates will be determined by finding the nearest neighbours of req_coords.
#' @param is_longlat A logical value indicating whether the coordinates are in longitude/latitude (TRUE) or in a projected coordinate system (FALSE).
#' @param remove29_02 Logical. Whether the 29/02 should be removed (default: TRUE).
#' @return A data frame containing the extracted weather data.
#' @export
#' @examples
#' \dontrun{
#' extract_weather_from_FMI("path/to/data", 2020:2021, my_polygon)
#' }
extract_weather_from_FMI <- function(pathdata, years, polygon = NULL, myCoords = NULL,
                                     vars = c("tday", "tmin", "tmax", "globrad", "rh", "rrday"), 
                                     varNames = c("Tday", "Tmin", "Tmax", "Globrad", "Rh", "RRday"), 
                                     id_vec = NULL, time_var = "Time",
                                     x_var = "Lon", y_var = "Lat", round_dec = 3, 
                                     req_nc_coords = NULL, is_longlat = FALSE, remove29_02 = TRUE) {
  
  # Checkmate tests
  checkmate::assert_character(pathdata, len = 1)
  checkmate::assert_integerish(years, lower = 1961)
  checkmate::assert_class(polygon, "SpatialPolygons", null.ok = TRUE)
  checkmate::assert_character(vars, min.len = 1)
  checkmate::assert_character(varNames, len = length(vars))
  checkmate::assert_numeric(round_dec, lower = 0)
  checkmate::assert_flag(is_longlat)
  
  if (is.null(polygon) & is.null(myCoords)) stop("Spatial input is needed. Provide a polygon or coordinates.")
  
  if (is.null(myCoords)) {
    ncName <- paste0(pathdata, vars[1], "_", years[1], ".nc")
    myCoords <- as.matrix(extr_coords_from_netCDF_within_plgn(ncName, polygon))
  }
  
  database <- list()
  
  for (i in seq_along(vars)) {
    ncNames <- paste0(pathdata, vars[i], "_", years, ".nc")
    
    extract_var <- lapply(ncNames, get_netcdf_by_nearest_coords, req_coords = myCoords, 
                          req_var = varNames[i], id_vec = id_vec, time_var = time_var,
                          x_var = x_var, y_var = y_var, round_dec = round_dec, 
                          req_nc_coords = req_nc_coords, is_longlat = is_longlat)
    
    database[[vars[i]]] <- data.table::rbindlist(extract_var)
    print(vars[i])
  } 
  
  # Merge all data frames in list
  database_all <- purrr::reduce(database, dplyr::full_join, by = c("id", "time", "x", "y"))
  
  # Remove 29/02
  if (remove29_02) {
    database_all <- database_all[!(lubridate::day(database_all$time) == 29 & lubridate::month(database_all$time) == 2), ]
  }

  # calculate photon flux density
  database_all$PPFD <- database_all$Globrad*0.48*4.6/1000
  
  # calculate VPD
  database_all[,VPD:=VPD_from_rh_tmin_tmax(Rh,Tmin,Tmax)]
  
  return(database_all)
}

#' Calculate Vapor Pressure Deficit (VPD) from Relative Humidity, Minimum and Maximum Temperature
#'
#' This function calculates the Vapor Pressure Deficit (VPD) using relative humidity (RH), minimum temperature (Tmin), and maximum temperature (Tmax).
#' Allen, RG, Pereira, LS, Raes, D., & Smith, M. (1998). Crop evapotranspiration-Guidelines for computing crop water requirements-FAO Irrigation and drainage paper 56. FAO, Rome , 300 (9), D05109.
#'
#' @param RH Relative Humidity in percentage (%)
#' @param Tmin Minimum Temperature in degrees Celsius (°C)
#' @param Tmax Maximum Temperature in degrees Celsius (°C)
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
