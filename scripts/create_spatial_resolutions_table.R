# Add a logical indicator (new column in data.table) that is TRUE when a 
# centre point in a point geometry with a different spatial resolution 
# (eg. 5-by-5 km) corresponds to a point in the original FMI coordinate set 
# (at 1-by-1 km resolution).


library(ncdf4)
library(data.table)
library(sf)


all_nc_files <- list.files(paste0(getwd(), "/data/nc"), full.names = T)
nc_fname <- grep(x = all_nc_files, pattern = "tday_1961.nc", value = T)
nc_ds <- nc_open(nc_fname)

dim_lon <- ncvar_get(nc_ds, "Lon")
dim_lat <- ncvar_get(nc_ds, "Lat")

nc_close(nc_ds)

dt <- as.data.table(expand.grid(x=dim_lon, y=dim_lat))

points_sf <- st_as_sf(dt, coords = c("x", "y"), crs = "+proj=utm +zone=35 +ellps=GRS80 +units=m +no_defs")

cell_size <- 500  # Adjust as needed

# Create a bounding box around the point
bbox <- st_bbox(points_sf)
bbox <- bbox + c(-cell_size, -cell_size, cell_size, cell_size)  # Expand the bbox

res_km <- 9
cell_size <- res_km*1000
res_name <- paste0("res_",res_km)

# Points in new resolution
points_new_res <- st_make_grid(st_as_sfc(bbox), cellsize = cell_size, what = "centers")

# Convert grid to sf object
points_new_res_sf <- st_as_sf(points_new_res)

# Create data.tables
points_dt <- data.table(x = st_coordinates(points_sf)[,1], 
                        y = st_coordinates(points_sf)[,2] )
new_res_centroids_dt <- data.table(x = st_coordinates(points_new_res_sf)[,1], 
                                y = st_coordinates(points_new_res_sf)[,2])


# Set TRUE if point is a centroid in new resolution grid
new_res_centroids_dt[, (res_name) := TRUE]
points_dt <- merge(points_dt, new_res_centroids_dt, by= c("x","y"), all.x = TRUE)
points_dt[, (res_name) := !is.na(get(res_name))]


old_res_dt1 <- copy(points_dt)

new_res_dt <- merge(old_res_dt1, points_dt, by= c("x","y"))

new_res_dt[res_5 == T & res_9 == T]


# save(new_res_dt, file = "data/rdata/fmi_resolutions_1_5_9_lookup_dt.rdata")










