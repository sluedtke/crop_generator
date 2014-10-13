#!/usr/bin/Rscript
### !/usr/local/R/R-3.1.0/bin/Rscript
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


library(doMPI)
cl = startMPIcluster()
registerDoMPI(cl)

temp=foreach(i=seq_len(nrow(nuts_info_all)),
	   	.combine="rbind",
		.packages=c("RODBCext", "plyr", "dplyr", "dplyrExtras", "reshape2", "foreach",
					"data.table"), 
		.errorhandling='pass'
		)%dopar%{
		
		#subset the dataframe
		nuts_info=nuts_info_all[i,]

		host=system('hostname', intern=T)

		temp=data.frame(res=paste("I am", i,  "on node",
								  host, "working on", nuts_info$nuts_code))
}
closeCluster(cl)

print(temp)
