##########################################
# Creates informative plots of pbias.
#
# Author: Richard Redweik
##########################################

rm(list=ls())

library(RODBCext)
library(ggplot2)
library(data.table)
library(dplyr)

# Query the database
query=readLines("./nuts_bias_data_source.sql")
print(query)
query=paste(query, collapse=" \n ")
conn=odbcConnect("crop_generator", uid="richard", case="postgresql")
bias_source=sqlExecute(conn, query, NULL, fetch=T)
odbcClose(conn)

# Calculate mean/median by year, (country)name, data_source, and nuts_id
bias_table <- data.table(bias_source)
mean_bias <- bias_table[, mean(pbias), by=list(year, name, data_source, nuts_id)]
setnames(mean_bias, "V1", "mean")
median_bias <- bias_table[, median(pbias), by=list(year, name, data_source, nuts_id)]
setnames(median_bias, "V1", "median")
min_bias <- bias_table[, min(pbias), by=list(year, name, data_source, nuts_id)]
setnames(min_bias, "V1", "min")
max_bias <- bias_table[, max(pbias), by=list(year, name, data_source, nuts_id)]
setnames(max_bias, "V1", "max")
min_max <- merge(min_bias, max_bias, by=c("year", "name", "data_source", "nuts_id"))
bias_nuts_year <- merge(min_max, mean_bias, by=c("year", "name", "data_source", "nuts_id"))

bias_nuts_year_median <- merge(min_max, median_bias, by=c("year", "name", "data_source", "nuts_id"))

mean_bias_per_year <- bias_table[, mean(pbias), by=list(name, data_source, nuts_id)]
setnames(mean_bias_per_year, "V1", "mean")

mean_year_15 <- mean_bias_per_year[abs(mean_bias_per_year$mean)>=15,]

# Plot nuts grouped by countries with abs(mena(pbias))>=15
ggplot(data=mean_year_15, aes(x=nuts_id, y=mean, fill=data_source)) + 
	geom_bar(stat="identity", width=.7, position='dodge') + 
	xlab("nuts_id") + 
	ylab("pbias") + 
	ggtitle("avg(pbias) of nuts with abs(mean(pbias)) >=15" ) + 
	coord_cartesian(ylim=c(-50, 100)) + 
	facet_wrap( ~ name, scales="free", ncol=3)
ggsave(file="pbias_nuts_15.png", width=36, height=40)

# Calculate yearly mean per country
mean_year_country <- bias_table[, mean(pbias), by=list(year, name, data_source)]
setnames(mean_year_country, "V1", "mean")
min_year_country <- bias_table[, min(pbias), by=list(year, name, data_source)]
setnames(min_year_country, "V1", "min")
max_year_country <- bias_table[, max(pbias), by=list(year, name, data_source)]
setnames(max_year_country, "V1", "max")
min_max_year <- merge(min_year_country, max_year_country, by=c("year", "name", "data_source"))
country_year <- merge(min_max_year, mean_year_country, by=c("year", "name", "data_source"))

# Plot yearly mean of countries
ggplot(data=country_year, mapping=aes(x=year, y=mean, ymin=min, ymax=max)) + 
	geom_pointrange(aes(col = factor(data_source), shape=factor(data_source)), position=position_dodge(width=0.7)) + 
 	xlab("nuts_id") +
 	ylab("pbias") + 
 	ggtitle("Yearly mean pbias of countries") + 
 	coord_cartesian(ylim=c(-50, 100)) + 
 	facet_wrap( ~ name, scales="free", ncol=2)
ggsave(file="pbias_countries.png", width=36, height=40)

# Create plots per country...
countries <- unique(bias_nuts_year$name)
for (c in countries) {
	country_nuts <- bias_nuts_year[bias_nuts_year$name==c,]
	country_nuts$mean_threshold <- ifelse(abs(country_nuts$mean) < 15, "abs(mean)<15", "abs(mean)>=15")

	# plot yearly mean pbias per nuts
	title <- paste("Yearly mean pbias of nuts in ", c, sep="")
	ggplot(data=country_nuts, mapping=aes(x=year, y=mean, ymin=min, ymax=max)) + 
		geom_pointrange(aes(col = factor(data_source), shape=factor(data_source), linetype=factor(mean_threshold)), position=position_dodge(width=0.7)) + 
		xlab("nuts_id") +
		ylab("pbias") + 
		ggtitle(title) + 
		coord_cartesian(ylim=c(-50, 100)) + 
		facet_wrap( ~ nuts_id, scales="free", ncol=2)
	filename <- paste("pbias_nuts_mean_", c, ".png", sep="")
	ggsave(file=filename, width=36, height=40)

	# plot yearly mean of nuts with abs(mean(pbias))>=15
	title <- paste("Yearly mean pbias of nuts in ", c, " with abs(mean(pbias))>=15.", sep="")
	# Get nuts with an overall mean greater +-15
	nuts_15 <- country_nuts[country_nuts$nuts_id %in% mean_year_15$nuts_id, ]
	if (dim(nuts_15)[1] > 0) {
		ggplot(data=nuts_15, mapping=aes(x=year, y=mean, ymin=min, ymax=max)) + 
			geom_pointrange(aes(col = factor(data_source), shape=factor(data_source), linetype=factor(mean_threshold)), position=position_dodge(width=0.7)) + 
 			xlab("nuts_id") +
 			ylab("pbias") + 
 			ggtitle(title) + 
 			coord_cartesian(ylim=c(-50, 100)) + 
 			facet_wrap( ~ nuts_id, scales="free", ncol=2)
 		filename <- paste("pbias_nuts_mean_15_", c, ".png", sep="")
 		ggsave(file=filename, width=36, height=40)
	}

	# plot yearly median of nuts with abs(mean(pbias))>=15
	country_nuts_median <- bias_nuts_year_median[bias_nuts_year_median$name==c,]
	country_nuts_median$mean_threshold <- ifelse(abs(country_nuts_median$median) < 15, "abs(median)<15", "abs(median)>=15")
	title <- paste("Yearly median pbias of nuts in ", c, sep="")
	ggplot(data=country_nuts_median, mapping=aes(x=year, y=median, ymin=min, ymax=max)) + 
		geom_pointrange(aes(col = factor(data_source), shape=factor(data_source), linetype=factor(mean_threshold)), position=position_dodge(width=0.7)) + 
		xlab("nuts_id") +
		ylab("pbias") + 
		ggtitle(title) + 
		coord_cartesian(ylim=c(-50, 100)) + 
		facet_wrap( ~ nuts_id, scales="free", ncol=2)
	filename <- paste("pbias_nuts_median_", c, ".png", sep="")
	ggsave(file=filename, width=36, height=40)

	# plot pbias of all crops per nuts
	country_nuts <- bias_source[bias_source$name==c,]
	filename <- paste("pbias_crops_nuts_", c, ".png", sep="")
	ggplot(data=country_nuts, aes(x=year, y=pbias, color=crop_short)) + 
		geom_line(aes(linetype=factor(data_source))) + 
		xlab("nuts_id") + 
		ylab("pbias") + 
		ggtitle("pbias of crops per nuts" ) + 
		coord_cartesian(ylim=c(-50, 100)) + 
		facet_wrap( ~ nuts_id, scales="free", ncol=3)
	ggsave(file=filename, width=36, height=49)

	
	country_nuts_dt <- data.table(country_nuts)
	crop_mean <- country_nuts_dt[, mean(pbias), by=list(nuts_id, crop_id, crop_short)]
	crop_15 <- crop_mean[abs(crop_mean$V1)>=15,]
	country_nuts_15 <- merge(country_nuts, crop_15, by=c("nuts_id", "crop_short")) 
	filename <- paste("pbias_crops_nuts_15_", c, ".png", sep="")
	ggplot(data=country_nuts_15, aes(x=year, y=pbias, color=crop_short)) + 
		geom_line(aes(linetype=factor(data_source))) + 
		xlab("nuts_id") + 
		ylab("pbias") + 
		ggtitle("pbias of crops per nuts with abs(mean(pbias))>=15" ) + 
		coord_cartesian(ylim=c(-50, 100)) + 
		facet_wrap( ~ nuts_id, scales="free", ncol=3)
	ggsave(file=filename, width=36, height=49)

	
}


