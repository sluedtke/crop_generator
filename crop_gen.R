################################################################################################

#       Filename: crop_gen.R

#       Author: Stefan Lüdtke

#       Created: Wednesday 11 June 2014 22:11:06 CEST

#       Last modified: Monday 22 September 2014 17:57:56 CEST

################################################################################################


################################################################################################
rm(list=ls())

################################################################################################
library(plyr)
library(dplyr)
library(reshape2)
library(foreach)
library(hydroGOF)
library(data.table)

################################################################################################
source("./crop_gen_functions.R")
###############################################################################################


# -------------------------- Data import --------------------------------#

ts_data=nuts_ts(nuts_id="DE41")
base_probs=nuts_probs(nuts_id="DE41")

years=unique(ts_data$year)

# -------------------------- convert to data.tables ---------------------#

ts_data=as.data.table(ts_data) 
setkey(ts_data, crop_id, year)

base_probs=as.data.table(base_probs)
setkey(base_probs, current_crop_id, objectid)

# -----------------------------------------------------------------------#

source("./crop_gen_functions.R")
temp=crop_distribution(nuts_base_probs=base_probs, nuts_base_ts=ts_data, years=years)

test=group_by(temp, crop_id, year) %>%
		summarize(., point=n()) %>%
		group_by(., year) %>%
		mutate(., ratio=point/sum(point)) %>%
		inner_join(., ts_data) %>%
		select(., -point) %>%
		group_by(., crop_id, year) %>%
		mutate(., percent_bias=pbias(ratio, value, na.rm=T))

max(test$percent_bias)








