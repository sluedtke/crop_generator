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
conn=odbcConnect("crop_generator", uid="sluedtke", case="postgresql")

nuts_id = "DE41"
nuts=data.frame('nuts_id'=nuts_id)

nuts_version=function(nuts_id){
		query=readLines("./nuts_version.sql")
		query=paste(query, collapse=" \n ")

        conn=odbcConnect("crop_generator", uid="sluedtke", case="postgresql")
		nuts_version=sqlExecute(conn, query, nuts_id, fetch=T)$max
		return(nuts_version)
		odbcClose(conn)
}

nuts_probs=function(nuts_id, nuts_version){
		query=readLines("./probability.sql")
		query=paste(query, collapse=" \n ")
		query=gsub("2010", nuts_version(nuts_id), query)

		parameters=data.frame(a=nuts_id, b=nuts_id)
        conn=odbcConnect("crop_generator", uid="sluedtke", case="postgresql")
		nuts_probs=sqlExecute(conn, query, parameters,  fetch=T)
		odbcClose(conn)
		return(nuts_probs)
}
