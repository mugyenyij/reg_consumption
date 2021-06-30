#!/bin/bash
#
#SBATCH --job-name=batched_quarterly
#SBATCH --output=quarterly_%j.out  # output file
#SBATCH -e res_%j.err        # File to which STDERR will be written
#SBATCH --partition=longq    # Partition to submit to 
#SBATCH --mem-per-cpu=5000
#SBATCH --cores=4

srun python batched_quarterly_consumption.py  $1
