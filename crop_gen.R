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

# And the crops and their minimum offset 
offset_tab=offset_year()


single_nuts=sample_n(nuts_info_all, 2, replace=F)
# nuts_info=nuts_info_all[nuts_info_all$nuts_code=='DE42', ]
# single_nuts=nuts_info_all[(nuts_info_all$nuts_code=='DE42' | nuts_info_all$nuts_code=='DE41'), ]

mc_runs=10

library(doMPI)
cl = startMPIcluster()
registerDoMPI(cl)


foreach(i=seq_len(nrow(single_nuts)),
		.packages=c("RODBCext", "plyr", "dplyr", "dplyrExtras", "reshape2", "foreach",
					"data.table", "lhs")
		)%dopar%{
		
		#subset the dataframe
		nuts_info=single_nuts[i,]

		# -------------------------- Data import --------------------------------#

		ts_data=nuts_ts(nuts_id=nuts_info$nuts_code)
		base_probs=nuts_probs(nuts_info)

		years=unique(ts_data$year)

		# -------------------------- convert to data.tables ---------------------#

		ts_data=as.data.table(ts_data) 
		setkey(ts_data, crop_id, year)

		base_probs=as.data.table(base_probs)
		setkey(base_probs, current_crop_id, objectid)


		offset_tab=as.data.table(lapply(offset_tab, as.numeric))
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

				# upload_data(nuts_info, data=mc_temp, prefix="stat")
		}
}

closeCluster(cl)

