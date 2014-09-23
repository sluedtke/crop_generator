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


# ------------------------  utility functions   ------------------------#

# rescale probs to sum up to one
rescale_probs=function(vect){
		vect=vect/sum(vect)
		return(vect)
}

# apply the parameter for the soil probability 
soil_para_call=function(prob, soil_para){

		# the difference that corresponds to fill to 100 %
		gap=1-prob
		# compute how much we fill based  the parameter
		prob=prob+(gap*soil_para)
		return(prob)
}

# apply the parameter for the follow up crop  probability 
follow_up_crop_para_call=function(prob, follow_up_crop_para){

		# the difference that corresponds to fill to 100 %
		gap=1-prob
		# compute how much we fill based  the parameter
		prob=prob+(gap*follow_up_crop_para)
		return(prob)
}


# ------------------ crop dist functions --------------------------#

nuts_crop_init=function(nuts_base_probs, nuts_base_ts, start_year, soil_para){
		
		## subset the time series for the very first year
		temp_ts=filter(nuts_base_ts, year==start_year) %>%
				rename(., c("crop_id"="current_crop_id"))

		## distribute the crops for the first year
		temp=subset(nuts_base_probs, nuts_base_probs$current_crop_id %in% temp_ts$current_crop_id) %>%
				select(., objectid, current_crop_id, current_soil_prob) %>%
				unique(., by=c("objectid", "current_crop_id")) %>%
				group_by(., objectid) %>%
				inner_join(., temp_ts, copy=T) %>%

				# rescale the follow up crop probs
				mutate(., value=rescale_probs(value)) %>%

				# rescale the current crop soil probs
				mutate(., current_soil_prob=rescale_probs(current_soil_prob)) %>%
				mutate(., current_soil_prob=soil_para_call(current_soil_prob, soil_para)) %>%

				# get the joint probability  and rescale
				mutate(., prob=rescale_probs(current_soil_prob*value)) %>%
				sample_n(., size=1, weight=prob, replace=F) %>%
				select(., c(objectid, current_crop_id, year))
		return(temp)
}

nuts_crop_cont=function(current_year, current_crop_dist, nuts_base_probs, nuts_base_ts,
						soil_para, follow_up_crop_para){

		## subset the time series for the very first year
		temp_ts=filter(nuts_base_ts, year==current_year) %>%
				rename(., c("crop_id"="follow_up_crop_id"))

		## distribute the crops for the current year
		temp=inner_join(current_crop_dist, base_probs) %>%
				select(., c(-current_soil_prob, -year)) %>%
				inner_join(., temp_ts, copy=T) %>%
				group_by(., objectid) %>%

				# rescale the time series probs
				mutate(., value=rescale_probs(value)) %>%

				# rescale the follow up crop probs
				mutate(., follow_up_crop_prob=rescale_probs(follow_up_crop_prob)) %>%
				mutate(., follow_up_crop_prob=follow_up_crop_para_call(follow_up_crop_prob, follow_up_crop_para)) %>%

				# rescale the follow up crop soil probs
				mutate(., follow_up_soil_prob=rescale_probs(follow_up_soil_prob)) %>%
				mutate(., follow_up_soil_prob=soil_para_call(follow_up_soil_prob, soil_para)) %>%

				# get the joint probability 
				mutate(., prob=rescale_probs(follow_up_crop_prob*follow_up_soil_prob*value)) %>%

				# and rescale
				sample_n(., size=1, weight=prob, replace=F) %>%

				select(., c(objectid, follow_up_crop_id, year)) %>%
				rename(.,  c("follow_up_crop_id"="current_crop_id"))

		return(temp)
}

crop_distribution=function(nuts_base_probs, nuts_base_ts, years, soil_para, follow_up_crop_para){

		# initialize for the first year
		current_year=years[1]
		init_crop_dist=nuts_crop_init(nuts_base_probs=nuts_base_probs,
									  nuts_base_ts=nuts_base_ts,
									 start_year=current_year,
									 soil_para=soil_para)

		# save the initialization
		current_crop_dist=init_crop_dist

		# strip the first entry from years
		years=years[-1]

		temp=foreach(current_year=years, .combine="rbind") %do% {
				current_crop_dist=nuts_crop_cont(current_year,
												 current_crop_dist=current_crop_dist,
												 nuts_base_probs=nuts_base_probs,
												 nuts_base_ts=nuts_base_ts,
												 soil_para=soil_para,
												 follow_up_crop_para=follow_up_crop_para)
		}
		
		temp=rbind(init_crop_dist, temp) %>%
				rename(., c("current_crop_id"="crop_id"))

		return(temp)
		
}
# ------------------------------------------------------------------#



