################################################################################################

#       Filename: crop_gen.R

#       Author: Stefan Lüdtke

#       Created: Wednesday 11 June 2014 22:11:06 CEST

################################################################################################


################################################################################################
rm(list=ls())
# options(error=recover, verbose=TRUE)
################################################################################################
# Initialize MPI
library("Rmpi")

# Notice we just say "give us all the slaves you've got."

if (mpi.comm.size() < 2) {
		print("More slave processes are required.")
		mpi.quit()
}else{}

################################################################################################
library(plyr)
library(dplyr)
library(dplyrExtras)
library(reshape2)
library(data.table)
library(RODBCext)
library(foreach)

############################################################################
source("./crop_gen_functions.R")
############################################################################

# -----------------------------------------------------------------------#
mpiwrapper = function() {
		####################################################################
		library(plyr)
		library(dplyr)
		library(dplyrExtras)
		library(reshape2)
		library(data.table)
		library(RODBCext)
		library(foreach)

		####################################################################
		source("./crop_gen_functions.R")
		####################################################################

		# Note the use of the tag for sent messages: 
		#     1=ready_for_task, 2=done_task, 3=exiting 
		# Note the use of the tag for received messages: 
		#     1=task, 2=done_tasks 
		junk = 0 
		done = 0 
		while (done != 1) {
				# Signal being ready to receive a new task 
				mpi.send.Robj(junk, 0, 1) 

				# Receive a task 
				task = mpi.recv.Robj(mpi.any.source(),mpi.any.tag()) 

				# 2 dim int vector. The first integer is the source and the second is
				# the tag.
				task_info = mpi.get.sourcetag() 
				tag = task_info[2] 

				# 1= we got something to do ..
				if (tag == 1) {
						# get the data from the r-object ..
						id = task$id

						# !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
						# !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
						# !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!

						# the actual job ... 

						# -------------------------- Data import --------------------------------#
						## fetching standard data

						nuts_info=as.data.frame(t(nuts_info_all[id,]))

						# And the crops and their minimum offset 
						offset_tab=offset_year()

						# And the crops and their maximum continuous occurrence 
						max_tab=max_year() %>%
								rename(., follow_up_crop_id = current_crop_id) %>%
								rename(., max_seq_year = offset_year) 

						ts_data=nuts_ts(nuts_id=nuts_info$nuts_code) %>%
								mutate(., value=as.numeric(as.character(value)))


						nuts_base_probs=nuts_probs(nuts_info) %>%
								mutate(., current_soil_prob=as.numeric(as.character(current_soil_prob))) %>%
								mutate(., follow_up_soil_prob=as.numeric(as.character(follow_up_soil_prob)))

						years=unique(ts_data$year)

						# -------------------------- convert to data.tables ---------------------#

						ts_data=as.data.table(ts_data) %>%
						# mutate  the numeric, in case we got only one value per year, the value is stored
						# a integer and that causes problems with the latter joins .. 
								mutate(., value=as.numeric(value))

						setkey(ts_data, crop_id, year)

						nuts_base_probs=as.data.table(nuts_base_probs)
						setkey(nuts_base_probs, current_crop_id, objectid)

						offset_tab=as.data.table(lapply(offset_tab, as.numeric))
						max_tab=as.data.table(lapply(max_tab, as.numeric))
						
						# -----------------------------------------------------------------------#


						# Some nuts unit are not covered by the EU-refgrid, the have 0 rows in the base_probs tab
						# we check for that and break the loop if that is the case
						if(nrow(nuts_base_probs)==0){
								mc_temp="Nuts unit not covered by the EU-RefGrid"
						}else{

								mc_temp=crop_distribution(nuts_base_probs=nuts_base_probs, nuts_base_ts=ts_data,
														  years=years, soil_para=1, follow_up_crop_para=1,
														  offset_tab=offset_tab, max_tab=max_tab)

								# joining the unique identifier from the postgres table
								mc_temp$oid_nuts=as.integer(as.character(nuts_info$oid_nuts))
								mc_temp$prob=as.integer(1)

								# upload the features to the DB
								upload_data(nuts_info, data=mc_temp, prefix="stat")

						}

						# !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
						# !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
						# !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!

						# Send a results message back to the master
						# we send just something to get an idea 
						results = list(result=as.character(nuts_info$nuts_code), id=id)

						mpi.send.Robj(results, 0, 2)
				}
				## We got nothing to do, all tasks done 
				else if (tag == 2) {
						# set done to 1, so the loop is not repeated
						done = 1
				}
				# We'll just ignore any unknown messages
		}
		mpi.send.Robj(junk, 0, 3)
}

# Disable idx on results table
drop_index_crop_gen_stat()

## Get the list of nuts we want to use
nuts_info_all=nuts()


## Broadcast the info the slaves
nuts_info_all=as.matrix(nuts_info_all)
mpi.bcast.Robj2slave(nuts_info_all)


# Create task list
tasks = vector('list')
for (i in 1:nrow(nuts_info_all)) {
    tasks[[i]] = list(id=i)
}

# Send the function to the slaves
mpi.bcast.Robj2slave(mpiwrapper)

# Call the function in all the slaves to get them ready to
# undertake tasks
mpi.bcast.cmd(mpiwrapper())

# Create data structure to store the results
result = as.list(rep(NA, length(tasks)))

junk = 0 
closed_slaves = 0 
n_slaves = mpi.comm.size()-1 

# -----------------------------------------------------------------------#
while (closed_slaves < n_slaves) { 
		# Receive a message from a slave 
		message = mpi.recv.Robj(mpi.any.source(), mpi.any.tag()) 

		# ‘mpi.get.sourcetag’ finds the source and tag of a received message.
		# 2 dim int vector. The first integer is the source and the second is
		# the tag.

		message_info = mpi.get.sourcetag() 

		slave_id = message_info[1] 
		tag = message_info[2] 

		# Note the use of the tag for sent messages: 
		#     1=ready_for_task, 2=done_task, 3=exiting 
		# Note the use of the tag for received messages: 
		#     1=task, 2=done_tasks 

		if (tag == 1) { 
				# slave is ready for a task. Give it the next task, or tell it tasks 
				# are done if there are none. 
				if (length(tasks) > 0) { 
						# Send a task,  
						mpi.send.Robj(tasks[[1]], slave_id, 1); 
						# and then remove it from the task list
						tasks[[1]] = NULL 
				} 
				else { 
						mpi.send.Robj(junk, slave_id, 2) 
				} 
		} 
		else if (tag == 2) { 
				# The message contains results. Do something with the results. 
				# Store them in the data structure
				id = message$id
				result[[id]] = message$result
		} 
		else if (tag == 3) { 
				# A slave has closed down. 
				closed_slaves=closed_slaves + 1 
		} 
} 

# Create index on results table
create_index_crop_gen_stat()

print(result)

mpi.close.Rslaves()
mpi.quit(save="no")

