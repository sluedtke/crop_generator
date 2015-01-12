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
		nuts_version=executeSQLQuery(query, NULL);
        return(nuts_version)
}

# ------------------------------------- #

nuts_probs=function(nuts_info){
		query=readLines("./probability.sql")
		query=paste(query, collapse=" \n ")
		query=gsub("XXXX", nuts_info$version, query)

		parameters=data.frame(a=nuts_info$nuts_code, b=nuts_info$nuts_code)
		nuts_probs=executeSQLQuery(query, parameters)
		return(nuts_probs)
}

# ------------------------------------- #

nuts_ts=function(nuts_id){
		query=readLines("./time_series.sql")
		query=paste(query, collapse=" \n ")
		nuts_ts=executeSQLQuery(query, nuts_id)
		return(nuts_ts)
}

# ------------------------------------- #

offset_year=function(){
		query=paste0('SELECT * FROM data.crop_offset_year;')
		offset_tab=executeSQLQuery(query, NULL)
		return(offset_tab)
}

# ------------------------------------- #

max_year=function(){
		query=paste0('SELECT * FROM data.crop_max_year;')
		max_tab=executeSQLQuery(query, NULL)
		return(max_tab)
}
# ------------------------------------- #

upload_data=function(nuts_info, data, prefix) {

		data=as.data.frame(data, stringsAsfactors=F)
		data$oid_nuts=as.integer(data$oid_nuts)
		data$crop_id=as.integer(data$crop_id)

		source('~/.rpostgres_ini.R')

		tab_name=paste0(paste0(prefix, '_'),
						paste(c(as.integer(as.character(nuts_info$oid_nuts)),
						as.integer(as.character(nuts_info$version))), sep="_", collapse="_"))

		schema_name = 'tmp'
		tab_name = toString(tab_name)
		schema_tab_name = paste(schema_name, tab_name, sep=".")
		query = paste0('DROP TABLE IF EXISTS ', schema_tab_name, ';');
		rs = dbSendQuery(con, query)
		
		if(dbWriteTable(con, c(schema_name, tab_name), data)==TRUE){
				nuts_success(nuts_info, tab_name, success=TRUE)
		}else{
				nuts_success(nuts_info, tab_name, success=FALSE)
		}

		dbDisconnect(con)
		dbUnloadDriver(drv)

}

# ------------------------------------- #
nuts_success=function(nuts_info, tab_name, success){

		success_tab=data.frame(oid_nuts=nuts_info$oid_nuts,
							   success_time=as.character(Sys.time()),
							   tmp_table=tab_name, success=success)

		query=readLines("./upsert_success.sql")
		query=paste(query, collapse=" \n ")
		nuts_ts=executeSQLQuery(query, success_tab, fetch=F)
		return()
}


# ------------------------  utility functions   ------------------------#

# rescale probs to sum up to one
rescale_probs=function(vect){
		vect=as.numeric(vect/sum(vect, na.rm=T))
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

# ------------------------------------- #

# summarize the monte-carlo simulations
summarize_mc=function(mc_data_table){

		temp=group_by(mc_data_table, objectid, year, crop_id) %>%
				# summarize(., count=n())
				summarise (n = n()) %>%
				mutate(prob = n / sum(n, na.rm=T)) %>%
				select(., c(objectid, year, crop_id, prob)) %>%
				filter(prob==max(prob)) %>%
				ungroup(.) %>%
				group_by(., objectid, year) %>%
				sample_n(., size=1)

		return(temp)
}

# ------------------------------------- #

# Wrapper function to call sqlExecute.
executeSQLQuery=function(query, parameters, fetch=T) {
		conn=odbcConnect("crop_generator", uid="sluedtke", case="postgresql")
		tryCatch(
			{
				result<-sqlExecute(conn, query, parameters, fetch, errors=TRUE)
			}, 
			error=function(e) {
				stop(e$message)
			}, 
			finally={
				odbcClose(conn)
			}
		) 
		return(result)
}

# ------------------ crop dist functions --------------------------#

nuts_crop_init=function(nuts_base_probs, nuts_base_ts, start_year, soil_para, offset_tab){
		
		## subset the time series for the very first year
		temp_ts=filter(nuts_base_ts, year==start_year) %>%
				rename(., c("crop_id"="current_crop_id"))

		## distribute the crops for the first year
		init_crop=subset(nuts_base_probs, nuts_base_probs$current_crop_id %in% temp_ts$current_crop_id) %>%
				select(., c(objectid, current_crop_id, current_soil_prob)) %>%
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


		# Set up the crop counter
		crop_counter=select(init_crop, c(objectid, current_crop_id)) %>%
				mutate(counter=1) %>%
				rename(., c("current_crop_id" = "follow_up_crop_id"))

		temp=list(crop_dist=init_crop, crop_offset=init_offset, crop_counter=crop_counter)

		return(temp)
}

# ------------------------------------- #

nuts_crop_cont=function(current_year, current_crop_dat, nuts_base_probs, nuts_base_ts,
						soil_para, follow_up_crop_para, offset_tab, max_tab){

		## subset the time series for the very first year
		temp_ts=filter(nuts_base_ts, year==current_year) %>%
				rename(., c("crop_id"="follow_up_crop_id"))
		

		# get the single items from the list
		current_crop_dist=current_crop_dat$crop_dist
		crop_offset=current_crop_dat$crop_offset
		crop_counter=current_crop_dat$crop_counter

		## distribute the crops for the current year
		temp_dist=inner_join(current_crop_dist, nuts_base_probs) %>%
 				select(., c(-current_soil_prob, -year)) %>%
				# join the time series data
				inner_join(., temp_ts, copy=T) %>%
				# join the offset info
				left_join(crop_offset, copy=T) %>%
				# update the probs if offset_year is present
				mutate_if(., is.na(offset_year)==F, 
						  value=0.01, follow_up_crop_prob=0.01, follow_up_soil_prob=0.01) %>%

				left_join(crop_counter, copy=T) %>%
				left_join(max_tab, copy=T) %>%

				# compute the difference between the maximum allowed sequence and the counter
				mutate(., temp=counter-max_seq_year) %>%

				# update the probs if the  difference between the counter and the maximum
				# of follow up years from the table is greater than 0
				mutate_if(., (temp>=0), 
						  value=0.01, follow_up_crop_prob=0.01, follow_up_soil_prob=0.01) %>%
				
				# building groups per pixel
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
				# and rescale
				mutate(., prob=rescale_probs(follow_up_crop_prob*follow_up_soil_prob*value)) %>%
		
				# draw
				sample_n(., size=1, weight=prob, replace=F)

		# Update the crop counter
		crop_counter=select(temp_dist, c(objectid, current_crop_id, follow_up_crop_id, counter)) %>%
		# If we selected the same crop as the year before, increase the counter by 1
				mutate_if(., current_crop_id==follow_up_crop_id, counter=counter+1) %>%
		# if not, set the counter to 0
				mutate_if(., current_crop_id!=follow_up_crop_id, counter=0) %>%
				select(., c(objectid, follow_up_crop_id, counter)) %>%
				rename(., c("follow_up_crop_id"="current_crop_id")) 
				
				# select columns of interest
		temp_dist=select(temp_dist, c(follow_up_crop_id, objectid, year)) %>%
				rename(., c("follow_up_crop_id"="current_crop_id"))

		# get the most recent cells and crops that must have a break
		current_crop_offset=left_join(temp_dist, offset_tab, copy=T) %>%
				select(., objectid, current_crop_id, offset_year) %>%
				rename(., c("current_crop_id" = "follow_up_crop_id")) %>%
				rbind(., crop_offset=mutate(crop_offset, offset_year=offset_year-1)) %>%
				filter(., is.na(offset_year)==F ) %>%
				filter(., offset_year!=0 )

		# create the return list 
		temp=list(crop_dist=temp_dist, crop_offset=current_crop_offset,
				  crop_counter=crop_counter)

		return(temp)
}

# ------------------------------------- #

crop_distribution=function(nuts_base_probs, nuts_base_ts, years, soil_para,
						   follow_up_crop_para, offset_tab, max_tab){
		# initialize for the first year
		current_year=years[1]
		init_crop_dat=nuts_crop_init(nuts_base_probs=nuts_base_probs,
									  nuts_base_ts=nuts_base_ts,
									 start_year=current_year,
									 soil_para=soil_para, 
									 offset_tab=offset_tab)

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
												follow_up_crop_para=follow_up_crop_para,
												offset_tab=offset_tab,
											   	max_tab=max_tab)

				current_crop_dist=current_crop_dat$crop_dist
		}

		# combine with the initialization
		temp=rbind(init_crop_dat$crop_dist, temp, use.names=T) %>%
				rename(., c("current_crop_id"="crop_id"))

		return(temp)
		
}

# ------------------------------------------------------------------#
# ------------------------------------------------------------------#

