#!/bin/bash
#
#SBATCH --job-name=chuncked_grouped_data
#SBATCH --output=chuncked_grouped_data_%j.out  # output file
#SBATCH -e res_%j.err        # File to which STDERR will be written
#SBATCH --partition=longq    # Partition to submit to 
#
#SBATCH --cores=4
#SBATCH --mem-per-cpu=5000

##srun python chuncked_grouped_data.py
srun python smoothed_chuncked_grped_data.py 
