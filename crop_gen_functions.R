################################################################################################

#       Filename: crop_gen_functions.R

#       Author: Stefan Lüdtke

#       Created: Wednesday 11 June 2014 22:10:15 CEST

################################################################################################

#	PURPOSE		########################################################################
#
#	Outsource the function for the probabilistic crop generator

################################################################################################


#!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
# RODBCext must be configured properly!!!
#!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!

################################################################################################

# ------------------------  BASIC queries   ------------------------#

nuts=function(){
		query=readLines("./nuts_version.sql")
		query=paste(query, collapse=" \n ")

        conn=odbcConnect("crop_generator", uid="sluedtke", case="postgresql")
		nuts_version=sqlExecute(conn, query, fetch=T)
		odbcClose(conn)
		return(nuts_version)
}

# ------------------------------------- #

nuts_probs=function(nuts_info){
		query=readLines("./probability.sql")
		query=paste(query, collapse=" \n ")
		query=gsub("XXXX", nuts_info$max, query)

		parameters=data.frame(a=nuts_info$nuts_code, b=nuts_info$nuts_code)
        conn=odbcConnect("crop_generator", uid="sluedtke", case="postgresql")
		nuts_probs=sqlExecute(conn, query, parameters,  fetch=T)
		odbcClose(conn)
		return(nuts_probs)
}

# ------------------------------------- #

nuts_ts=function(nuts_id){
		query=readLines("./time_series.sql")
		query=paste(query, collapse=" \n ")
        conn=odbcConnect("crop_generator", uid="sluedtke", case="postgresql")
		nuts_ts=sqlExecute(conn, query, nuts_id,  fetch=T)
		odbcClose(conn)
		return(nuts_ts)
}

# ------------------------------------- #

upload_data=function(nuts_info, data){

		# create the table name first
		tab_name=data.frame(tab_name=as.character(paste0('tmp.', 
						 paste("tab", nuts_info, sep="_", collapse="_"))))

        conn=odbcConnect("crop_generator", uid="sluedtke", case="postgresql")
		## drop table if exists
		query=paste0('DROP TABLE IF EXISTS ', tab_name$tab_name, ';')
		sqlExecute(conn, query, NULL, fetch=F)

		# save table 
		sqlSave(conn, dat=data, tablename=tab_name$tab_name, addPK=T, fast=T, safer=F)
		odbcClose(conn)
}


# ------------------------------------------------------------------#

# ------------------------  utility functions   ------------------------#

# rescale probs to sum up to one
rescale_probs=function(vect){
		vect=vect/sum(vect)
		return(vect)
}

# ------------------------------------- #

# apply the parameter for the soil probability 
soil_para_call=function(prob, soil_para){
		
		## just a conversion to get a better feeling for the parameter
		soil_para=1-soil_para
		# the difference that corresponds to fill to 100 %
		gap=1-prob
		# compute how much we fill based  the parameter
		prob=prob+(gap*soil_para)
		return(prob)
}

# ------------------------------------- #

# apply the parameter for the follow up crop  probability 
follow_up_crop_para_call=function(prob, follow_up_crop_para){

		## just a conversion to get a better feeling for the parameter
		follow_up_crop_para=1-follow_up_crop_para
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
		init_crop=subset(nuts_base_probs, nuts_base_probs$current_crop_id %in% temp_ts$current_crop_id) %>%
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

		init_offset=left_join(init_crop, offset_tab, copy=T) %>%
				select(., objectid, current_crop_id, offset_year) %>%
				rename(., c("current_crop_id" = "follow_up_crop_id"))

		temp=list(crop_dist=init_crop, crop_offset=init_offset)
		return(temp)
}

# ------------------------------------- #

nuts_crop_cont=function(current_year, current_crop_dat, nuts_base_probs, nuts_base_ts,
						soil_para, follow_up_crop_para){

		## subset the time series for the very first year
		temp_ts=filter(nuts_base_ts, year==current_year) %>%
				rename(., c("crop_id"="follow_up_crop_id"))

		# get the single items from the list
		current_crop_dist=current_crop_dat$crop_dist
		crop_offset=current_crop_dat$crop_offset


		## distribute the crops for the current year
		temp_dist=inner_join(current_crop_dist, base_probs) %>%
 				select(., c(-current_soil_prob, -year)) %>%
				# join the time series data
				inner_join(., temp_ts, copy=T) %>%
				# join the offset info
				left_join(crop_offset, copy=T) %>%
				# update the probs if offset_year is present
				mutate_if(., is.na(offset_year)==F, 
						  value=0, follow_up_crop_prob=0, follow_up_soil_prob=0) %>%

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
				
				# select columns of interest
				select(.,  c(follow_up_crop_id, objectid, year)) %>%
				rename(.,  c("follow_up_crop_id"="current_crop_id"))

		# get the most recent cells and crops that must have a break
		current_crop_offset=left_join(temp_dist, offset_tab, copy=T) %>%
				select(., objectid, current_crop_id, offset_year) %>%
				rename(., c("current_crop_id" = "follow_up_crop_id")) %>%
				rbind(., crop_offset=mutate(crop_offset, offset_year=offset_year-1)) %>%
				filter(., is.na(offset_year)==F ) %>%
				filter(., offset_year!=0 )



		temp=list(crop_dist=temp_dist, crop_offset=current_crop_offset)
		return(temp)
}

# ------------------------------------- #

crop_distribution=function(nuts_base_probs, nuts_base_ts, years, soil_para, follow_up_crop_para){

		# initialize for the first year
		current_year=years[1]
		init_crop_dat=nuts_crop_init(nuts_base_probs=nuts_base_probs,
									  nuts_base_ts=nuts_base_ts,
									 start_year=current_year,
									 soil_para=soil_para)

		# save the initialization
		current_crop_dat=init_crop_dat

		# strip the first entry from years
		years=years[-1]

		temp=foreach(current_year=years, .combine="rbind") %do% {

				current_crop_dat=nuts_crop_cont(current_year,
												current_crop_dat=current_crop_dat,
												nuts_base_probs=nuts_base_probs,
												nuts_base_ts=nuts_base_ts,
												soil_para=soil_para,
												follow_up_crop_para=follow_up_crop_para)

				current_crop_dist=current_crop_dat$crop_dist
		}

		# combine with the initialization
		temp=rbind(init_crop_dat$crop_dist, temp, use.names=T) %>%
				rename(., c("current_crop_id"="crop_id"))

		return(temp)
		
}

# ------------------------------------------------------------------#
# ------------------------------------------------------------------#

