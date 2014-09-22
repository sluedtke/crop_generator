################################################################################################

#       Filename: crop_gen_functions.R

#       Author: Stefan Lüdtke

#       Created: Wednesday 11 June 2014 22:10:15 CEST

################################################################################################

#	PURPOSE		########################################################################
#
#	Outsource the function for the probabilistic crop generator

################################################################################################
# setting variables to access the DB
# RODBCext must be configured properly!!!

library(RODBCext)

################################################################################################

# ------------------------  BASIC queries   ------------------------#

nuts_version=function(nuts_id){
		query=readLines("./nuts_version.sql")
		query=paste(query, collapse=" \n ")

        conn=odbcConnect("crop_generator", uid="sluedtke", case="postgresql")
		nuts_version=sqlExecute(conn, query, nuts_id, fetch=T)$max
		odbcClose(conn)
		return(nuts_version)
}

nuts_probs=function(nuts_id){
		query=readLines("./probability.sql")
		query=paste(query, collapse=" \n ")
		query=gsub("XXXX", nuts_version(nuts_id), query)

		parameters=data.frame(a=nuts_id, b=nuts_id)
        conn=odbcConnect("crop_generator", uid="sluedtke", case="postgresql")
		nuts_probs=sqlExecute(conn, query, parameters,  fetch=T)
		odbcClose(conn)
		return(nuts_probs)
}

nuts_ts=function(nuts_id){
		query=readLines("./time_series.sql")
		query=paste(query, collapse=" \n ")

        conn=odbcConnect("crop_generator", uid="sluedtke", case="postgresql")
		nuts_ts=sqlExecute(conn, query, nuts_id,  fetch=T)
		odbcClose(conn)
		return(nuts_ts)
}

# ------------------------------------------------------------------#


# ------------------------  util function   ------------------------#

rescale_probs=function(vect){
		vect=vect/sum(vect)
		return(vect)
}

# ------------------ crop dist functions --------------------------#


nuts_crop_init=function(nuts_base_probs, nuts_base_ts, start_year){
		
		## subset the time series for the very first year
		temp_ts=filter(nuts_base_ts, year==start_year) %>%
				rename(., c("crop_id"="current_crop_id"))

		## distribute the crops for the first year
		temp=subset(nuts_base_probs, nuts_base_probs$current_crop_id %in% temp_ts$current_crop_id) %>%
				select(., objectid, current_crop_id, current_soil_prob) %>%
				unique(., by=c("objectid", "current_crop_id")) %>%
				group_by(., objectid) %>%
				inner_join(., temp_ts, copy=T) %>%
				mutate(., prob=rescale_probs(current_soil_prob*value)) %>%
				group_by(., objectid) %>%
				sample_n(., size=1, weight=prob, replace=T) %>%
				select(., c(objectid, current_crop_id, year))
		return(temp)
}

nuts_crop_cont=function(current_year, current_crop_dist, nuts_base_probs, nuts_base_ts){

		## subset the time series for the very first year
		temp_ts=filter(nuts_base_ts, year==current_year) %>%
				rename(., c("crop_id"="follow_up_crop_id"))

		# print(current_year)
		# print(temp_ts)
		# if(current_year==1995){
		# 		browser()
		# }else{}
        #
		## distribute the crops for the current year
		temp=inner_join(current_crop_dist, base_probs) %>%
				select(., c(-current_soil_prob, -year)) %>%
				inner_join(., temp_ts, copy=T)# %>%
				group_by(., objectid) %>%
				summarize(., sum(value))
				mutate(., follow_up_crop_prob=rescale_probs(follow_up_crop_prob)) %>%
				mutate(., follow_up_soil_prob=rescale_probs(follow_up_soil_prob)) %>%
				mutate(., prob=rescale_probs(follow_up_soil_prob*follow_up_soil_prob*value)) %>%
				sample_n(., size=1, weight=prob, replace=T) %>%
				select(., c(objectid, follow_up_crop_id, year)) %>%
				rename(.,  c("follow_up_crop_id"="current_crop_id"))

		return(temp)
}

crop_distribution=function(nuts_base_probs, nuts_base_ts, years){

		# initialize for the first year
		current_year=years[1]
		init_crop_dist=nuts_crop_init(nuts_base_probs=base_probs, nuts_base_ts=ts_data,
										 start_year=current_year)

		# save the initialization
		current_crop_dist=init_crop_dist

		# strip the first entry from years
		years=years[-1]

		temp=foreach(current_year=years, .combine="rbind") %do% {
				current_crop_dist=nuts_crop_cont(current_year,
												 current_crop_dist=current_crop_dist,
												 nuts_base_probs=base_probs,
												 nuts_base_ts=ts_data)
		}
		
		temp=rbind(init_crop_dist, temp) %>%
				rename(., c("current_crop_id"="crop_id"))

		return(temp)
		
}
# ------------------------------------------------------------------#
