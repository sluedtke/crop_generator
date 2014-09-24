#!/usr/bin/Rscript
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
library(reshape2)

library(hydroGOF)
library(data.table)
library(lhs)

library(doMPI)
library(foreach)

library(RODBCext)

# we do not give a number here, which means we take as much as we
# get- managed by the startup script
cl=startMPIcluster()

# register cl for the foreach framework
registerDoMPI(cl) 	 


list_nuts_id=c('DE42', 'DE41', 'ITI4')

system.time(foreach(nuts_id=list_nuts_id)%dopar%{

		##############################################################
		source("./crop_gen_functions.R")
		##############################################################

		# -------------------------- Data import --------------------------------#

		ts_data=nuts_ts(nuts_id)
		base_probs=nuts_probs(nuts_id)

		years=unique(ts_data$year)

		# -------------------------- convert to data.tables ---------------------#

		ts_data=as.data.table(ts_data) 
		setkey(ts_data, crop_id, year)

		base_probs=as.data.table(base_probs)
		setkey(base_probs, current_crop_id, objectid)

		# -----------------------------------------------------------------------#

		temp=crop_distribution(nuts_base_probs=base_probs, nuts_base_ts=ts_data,
							   years=years, soil_para=0, follow_up_crop_para=0)

		test=group_by(temp, crop_id, year) %>%
				summarize(., point=n()) %>%
				group_by(., year) %>%
				mutate(., ratio=point/sum(point)) %>%
				inner_join(., ts_data) %>%
				group_by(., year) %>%
				mutate(., value=rescale_probs(value)) %>%
				select(., -point) %>%
				group_by(., year, crop_id) %>%
				mutate(., percent_bias=pbias(ratio, value, na.rm=T))#%>%

		max(test$percent_bias)

		test=group_by(test, year) %>%
				summarize(., mean=mean(percent_bias))
})
