library(tidyverse)
library(checkmate)
library(ncdf4)
library(data.table)
library(sp)
library(raster)
library(lubridate)

##load functions
source("src/functions.R")

###create a polygon just for testing
WGScoor<-  data.table(lon=c(300000,305000,305000,300000),
                      lat=c(6800000,6800000,6802000,6802000))
polygon1 <- Polygon(WGScoor)
polygons1 <- Polygons(list(polygon1), ID = "1")
sp_polygons <- SpatialPolygons(list(polygons1))
proj4string(sp_polygons)<- CRS("+init=epsg:3067")


pathdata <- "C:/Users/checc/Downloads/dailyData/"
# vars <- c("tday", "tmin","tmax","globrad","rh","rrday")
# varNames <- c("Tday", "Tmin","Tmax","Globrad","Rh","RRday")
years <- 2000:2001
# nc_fname <- c("C:/Users/checc/Downloads/dailyData/tday_2001.nc")
               

weather_in <- extract_weather_from_FMI(pathdata,years,polygon = sp_polygons)


weather_in <- extract_weather_from_FMI(pathdata,years,myCoords = gg)
weather_in
#### CO2 is missing
#### 29/02 are removed from leap years

#to do
#### units convertion (f/R)
#### VPD calculations from Rh (f/R)
#### convert to prebas input format (f/R)
#### add CO2 (S/R)
#### data need to be stored in allas (S/R)
#### modify for spatial points (F/S)
### change resolution (input in the function) (S/f)



