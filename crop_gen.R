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
library(RODBCext)

############################################################################
source("./crop_gen_functions.R")
############################################################################

## Get the list of nuts we want to use
nuts_info_all=nuts()

# And the crops and their minimum offset 
offset_tab=offset_year()

# And the crops and their maximum continuous occurrence 
max_tab=max_year() %>%
		rename(., c("current_crop_id" = "follow_up_crop_id")) %>%
		rename(., c("offset_year" = "max_seq_year"))


# single_nuts=sample_n(nuts_info_all, 2, replace=F)
single_nuts=nuts_info_all[(nuts_info_all$nuts_code=='UKF1'
						   |nuts_info_all$nuts_code=='UKF2' 
						   |nuts_info_all$nuts_code=='UKF3'), ]

mc_runs=1

library(doMPI)
cl = startMPIcluster(count=3, verbose=TRUE)
registerDoMPI(cl)


foreach(i=seq_len(nrow(single_nuts)),
		.packages=c("RODBCext", "plyr", "dplyr", "dplyrExtras", "reshape2", "foreach",
					"data.table"), 
		.errorhandling='pass'
		)%dopar%{
		
		#subset the dataframe
		nuts_info=single_nuts[i,]

		# -------------------------- Data import --------------------------------#

		ts_data=nuts_ts(nuts_id=nuts_info$nuts_code)
		base_probs=nuts_probs(nuts_info)

		years=unique(ts_data$year)

		# -------------------------- convert to data.tables ---------------------#

		ts_data=as.data.table(ts_data) %>%
		# mutate  the numeric, in case we got only one value per year, the value is stored
		# a integer and that causes problems with the latter joins .. 
				mutate(., value=as.numeric(value))
		
		setkey(ts_data, crop_id, year)

		base_probs=as.data.table(base_probs)
		setkey(base_probs, current_crop_id, objectid)


		offset_tab=as.data.table(lapply(offset_tab, as.numeric))
		max_tab=as.data.table(lapply(max_tab, as.numeric))
		# -----------------------------------------------------------------------#

		# Some nuts unit are not covered by the EU-refgrid, the have 0 rows in the base_probs tab
		# we check for that and break the loop if that is the case
		if(nrow(base_probs)==0){
				cat("Nuts unit not covered by the EU-RefGrid \n")
		}else{
				mc_temp=foreach(run=seq_len(mc_runs), .combine="rbind")%do%{

						temp=crop_distribution(nuts_base_probs=base_probs, nuts_base_ts=ts_data,
										  years=years, soil_para=1, follow_up_crop_para=1)
						temp$mc_run=factor(run)
						temp=temp
				}
				
				# summarize the mc runs
				mc_temp=summarize_mc(mc_temp)

				# joining the unique identifier from the postgres table
				mc_temp$oid_nuts=nuts_info$id
				
				# upload the features to the DB
				upload_data(nuts_info, data=mc_temp, prefix="stat")

		}
}

closeCluster(cl)
