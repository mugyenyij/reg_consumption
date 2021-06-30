#!/bin/bash
#
#SBATCH --job-name=batched_half_year
#SBATCH -o ./output/half_year_%j.out  # output file
#SBATCH -e ./STDERR/res_%j.err        # File to which STDERR will be written
#SBATCH --partition=longq    # Partition to submit to
#SBATCH --mem-per-cpu=5000
#SBATCH --cores=4

srun python batched_half_year_consumption_from_2015.py  $1
