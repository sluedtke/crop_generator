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

############################################################################
source("./crop_gen_functions.R")
############################################################################

mpiwrapper = function() {
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
						# the actual job ... 
						# nuts_info=as.data.frame(nuts_info_all[id,])
						nuts_info=as.data.frame(t(nuts_info_all[id,]))

						temp=data.frame(res=paste("I am ", id,  " on node " ,
												  mpi.get.processor.name(),
												  " working on ",
												  nuts_info$nuts_code))

						# Send a results message back to the master
						results = list(result=temp, id=id)
						mpi.send.Robj(results, 0, 2)
						# !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
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

## Get the list of nuts we want to use
nuts_info_all=nuts()

nuts_info_all=as.matrix(nuts_info_all)
mpi.bcast.Robj2slave(nuts_info_all)


# Create task list
tasks = vector('list')
for (i in 1:nrow(nuts_info_all)) {
# for (i in 1:length(nuts_info_all)) {
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
				result[id] = message$result
		} 
		else if (tag == 3) { 
				# A slave has closed down. 
				closed_slaves=closed_slaves + 1 
		} 
} 


print(result)

mpi.close.Rslaves()
mpi.quit(save="no")

