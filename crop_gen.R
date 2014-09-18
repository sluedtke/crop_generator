################################################################################################

#       Filename: crop_gen_functions.R

#       Author: Stefan Lüdtke

#       Created: Wednesday 11 June 2014 22:11:06 CEST

#       Last modified: Friday 29 August 2014 15:28:34 CEST

################################################################################################


################################################################################################
rm(list=ls())

################################################################################################
library(plyr)
library(reshape2)


library(raster)
library(zoo)
library(sp)
library(rgdal)

################################################################################################

# -------------------------- General settings ---------------------------#
data_path="../data/all_raster/"

# -------------------------- Data import --------------------------------#

test=readOGR(dsn="../data/NUTS_2010_10M_SH/Shape/data/stat_sep/",
	     layer="NUTS_RG_10M_2010_repro_STAT_LEVL___0_si")

spplot(test)


temp=raster()
# extent(xmin, xmax, ymin, ymax)
e=extent(2426378.0132, 6293974.6215, 1528101.2618, 5446513.5222)
extent(temp)=e
projection(temp)="+proj=laea +lat_0=52 +lon_0=10 +x_0=4321000 +y_0=3210000 +ellps=GRS80 +units=m +no_defs"
res(temp)=1000


a=rasterize(test, temp)

