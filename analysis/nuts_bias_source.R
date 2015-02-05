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
conn=odbcConnect("crop_generator", uid="richard", case="postgresql")
query=readLines("./nuts_bias_data_source.sql")
query=paste(query, collapse=" \n ")
bias_source=sqlExecute(conn, query, NULL, fetch=T)

query <- readLines("./time_series.sql")
query <- paste(query, collapse=" \n ")
time_series <- sqlExecute(conn, query, NULL, fetch=T)

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
ggplot(data=mean_year_15, aes(x=nuts_id, y=mean)) + 
	geom_bar(stat="identity", width=.7, position='dodge', fill="darkcyan") + 
	xlab("nuts_id") + 
	ylab("pbias") + 
	ggtitle("avg(pbias) of nuts with abs(mean(pbias)) >=15" ) + 
	coord_cartesian(ylim=c(-50, 100)) + 
	facet_wrap( ~ name, scales="free", ncol=3) +
	theme(text = element_text(size=10))
ggsave(file="figures/pbias_nuts_15.png", width=17, height=10)

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
	geom_pointrange(position=position_dodge(width=0.7)) + 
 	xlab("nuts_id") +
 	ylab("pbias") + 
 	ggtitle("Yearly mean pbias of countries") + 
 	coord_cartesian(ylim=c(-50, 100)) + 
 	facet_wrap( ~ name, scales="free", ncol=2) + 
	theme(text = element_text(size=10))
ggsave(file="figures/pbias_countries.png", width=15, height=10)

time_series_15 <- time_series[time_series$nuts_id %in% mean_year_15$nuts_id, ]

# Create plots per country...
countries <- unique(bias_nuts_year$name)
for (c in countries) {
	country_nuts <- bias_nuts_year[bias_nuts_year$name==c,]
	country_nuts$mean_threshold <- ifelse(abs(country_nuts$mean) < 15, "abs(mean)<15", "abs(mean)>=15")

	# plot yearly mean pbias per nuts
	title <- paste("Yearly mean pbias of nuts in ", c, sep="")
	ggplot(data=country_nuts, mapping=aes(x=year, y=mean, ymin=min, ymax=max)) + 
		geom_pointrange(aes(linetype=factor(mean_threshold)), position=position_dodge(width=0.7)) + 
		xlab("nuts_id") +
		ylab("pbias") + 
		ggtitle(title) + 
		coord_cartesian(ylim=c(-50, 100)) + 
		facet_wrap( ~ nuts_id, scales="free", ncol=2) +
		theme(text = element_text(size=10))
	filename <- paste("figures/pbias_nuts_mean_", c, ".png", sep="")
	ggsave(file=filename, width=15, height=20)

	# plot yearly mean of nuts with abs(mean(pbias))>=15
	title <- paste("Yearly mean pbias of nuts in ", c, " with abs(mean(pbias))>=15.", sep="")
	# Get nuts with an overall mean greater +-15
	nuts_15 <- country_nuts[country_nuts$nuts_id %in% mean_year_15$nuts_id, ]
	if (dim(nuts_15)[1] > 0) {
		ggplot(data=nuts_15, mapping=aes(x=year, y=mean, ymin=min, ymax=max)) + 
			geom_pointrange(aes(linetype=factor(mean_threshold)), position=position_dodge(width=0.7)) + 
			 xlab("nuts_id") +
			 ylab("pbias") + 
			 ggtitle(title) + 
			 coord_cartesian(ylim=c(-50, 100)) + 
			 facet_wrap( ~ nuts_id, scales="free", ncol=2) +
			 theme(text = element_text(size=10))
		 filename <- paste("figures/pbias_nuts_mean_15_", c, ".png", sep="")
		 ggsave(file=filename, width=15, height=20)
	}
	# plot yearly median of nuts with abs(mean(pbias))>=15
	country_nuts_median <- bias_nuts_year_median[bias_nuts_year_median$name==c,]
	country_nuts_median$mean_threshold <- ifelse(abs(country_nuts_median$median) < 15, "abs(median)<15", "abs(median)>=15")
	title <- paste("Yearly median pbias of nuts in ", c, sep="")
	ggplot(data=country_nuts_median, mapping=aes(x=year, y=median, ymin=min, ymax=max)) + 
		geom_pointrange(aes(linetype=factor(mean_threshold)), position=position_dodge(width=0.7)) + 
		xlab("nuts_id") +
		ylab("pbias") + 
		ggtitle(title) + 
		coord_cartesian(ylim=c(-50, 100)) + 
		facet_wrap( ~ nuts_id, scales="free", ncol=2) +
		theme(text = element_text(size=10))
	filename <- paste("figures/pbias_nuts_median_", c, ".png", sep="")
	ggsave(file=filename, width=15, height=10)

	# plot pbias of all crops per nuts
	country_nuts <- bias_source[bias_source$name==c,]
	filename <- paste("figures/pbias_crops_nuts_", c, ".png", sep="")
	ggplot(data=country_nuts, aes(x=year, y=pbias, color=crop_short)) + 
		geom_line() + 
		xlab("nuts_id") + 
		ylab("pbias") + 
		ggtitle("pbias of crops per nuts" ) + 
		coord_cartesian(ylim=c(-50, 100)) + 
		facet_wrap( ~ nuts_id, scales="free", ncol=3) +
		theme(text = element_text(size=10))
	ggsave(file=filename, width=9, height=10)


	country_nuts_dt <- data.table(country_nuts)
	crop_mean <- country_nuts_dt[, mean(pbias), by=list(nuts_id, crop_id, crop_short)]
	crop_15 <- crop_mean[abs(crop_mean$V1)>=15,]
	country_nuts_15 <- merge(country_nuts, crop_15, by=c("nuts_id", "crop_short")) 
	filename <- paste("figures/pbias_crops_nuts_15_", c, ".png", sep="")
	ggplot(data=country_nuts_15, aes(x=year, y=pbias, color=crop_short)) + 
		geom_line() + 
		xlab("nuts_id") + 
		ylab("pbias") + 
		ggtitle("pbias of crops per nuts with abs(mean(pbias))>=15" ) + 
		coord_cartesian(ylim=c(-50, 100)) + 
		facet_wrap( ~ nuts_id, scales="free", ncol=3) + 
		theme(text = element_text(size=10))
	ggsave(file=filename, width=9, height=10)

	ts_nuts_15 <- time_series_15[time_series_15$name==c, ]
	root_crops <- c(7, 9, 14)
	ts_nuts_15$root_crops <- ifelse(ts_nuts_15$crop_id %in% root_crops, "root crop", "non root crop")
	if (dim(ts_nuts_15)[1] > 0) {
		filename <- paste("figures/ts_boxplot_", c, ".png", sep="")
		ggplot(data=ts_nuts_15, aes(x=factor(crop_short), y=value)) +
			geom_boxplot(aes(colour=factor(root_crops))) +
			ggtitle("boxplot of values of crops per nuts region with abs(mean(pbias))>=15") +
			facet_wrap(~ nuts_id, scales="free", ncol=3) + 
			theme(text = element_text(size=10))
		ggsave(file=filename, width=20, height=15)
	}

}


