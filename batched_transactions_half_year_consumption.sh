#!/bin/bash
#
#SBATCH --job-name=batched_transactions_half_year
#SBATCH --output=transactions_half_year_%j.out  # output file
#SBATCH -e res_%j.err        # File to which STDERR will be written
#SBATCH --partition=longq    # Partition to submit to
#SBATCH --mem-per-cpu=5000
#SBATCH --cores=4

srun python batched_transactions_half_year_consumption.py $1
