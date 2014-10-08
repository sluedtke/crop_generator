#!/bin/bash
# show commands being executed, per debug
set -x

######################################################################

#       Filename: start_R.sh 

#       Author: Stefan Lüdtke

#       Created: Wednesday 24 September 2014 17:28:09 CEST

######################################################################
# Set job parameters

#BSUB -a openmpi

# Start R MPI job
mpirun.lsf ./crop_gen.R

######################################################################
