#!/usr/local/R/R-3.1.0/bin/Rscript
################################################################################################

#       Filename: crop_gen.R

#       Author: Stefan Lüdtke

#       Created: Wednesday 11 June 2014 22:11:06 CEST

################################################################################################


################################################################################################
rm(list=ls())

################################################################################################
library(plyr)
library(dplyr)
library(dplyrExtras)
library(reshape2)
library(foreach)
library(data.table)
library(lhs)
library(RODBCext)

############################################################################
source("./crop_gen_functions.R")
############################################################################

## Get the list of nuts we want to use
nuts_info_all=nuts()



single_nuts=sample_n(nuts_info_all, 20, replace=F)
# nuts_info=nuts_info_all[nuts_info_all$nuts_code=='DE42', ]
# single_nuts=nuts_info_all[(nuts_info_all$nuts_code=='DE42' | nuts_info_all$nuts_code=='DE41'), ]

mc_runs=1

library(doMPI)
cl = startMPIcluster()
registerDoMPI(cl)
#  
foreach(i=c(1:nrow(single_nuts)),
		.packages=c("RODBCext", "plyr", "dplyr", "dplyrExtras", "reshape2", "foreach",
					"data.table", "lhs")
		)%dopar%{
		
		#subset the dataframe
		nuts_info=single_nuts[i,]

		# -------------------------- Data import --------------------------------#

		ts_data=nuts_ts(nuts_id=nuts_info$nuts_code) %>%
				mutate(., value=as.numeric(as.character(value)))

		base_probs=nuts_probs(nuts_info) %>%
				mutate(., current_soil_prob=as.numeric(as.character(current_soil_prob))) %>%
				mutate(., follow_up_soil_prob=as.numeric(as.character(follow_up_soil_prob)))

		years=unique(ts_data$year)

		offset_tab=data.frame(current_crop_id=c(9, 7, 14), offset_year=c(3, 3, 3))

		# -------------------------- convert to data.tables ---------------------#

		ts_data=as.data.table(ts_data) 
		setkey(ts_data, crop_id, year)

		base_probs=as.data.table(base_probs)
		setkey(base_probs, current_crop_id, objectid)

		# -----------------------------------------------------------------------#

		temp=foreach(run=mc_runs, .combine="rbind")%do%{

				temp=crop_distribution(nuts_base_probs=base_probs, nuts_base_ts=ts_data,
								  years=years, soil_para=1, follow_up_crop_para=1)
				temp$mc_run=run
				temp=temp
		}

		upload_data(nuts_info, data=temp)
}

closeCluster(cl)

