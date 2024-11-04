#!/bin/bash

#PBS -N CAR_split
#PBS -l select=1:ncpus=40:mpiprocs=40
#PBS -l walltime=96:00:00
#PBS -q atlas
#PBS -o /home/pquilici/gitworkspace/split_offdb/logs/output.log
#PBS -e /home/pquilici/gitworkspace/split_offdb/logs/error.log
#PBS -m abe
#PBS -M paq.coutinho@gmail.com

# Carrega o módulo de uma versão mais recente do Python e MPI
module load gcc python/3.9.17 openmpi # Certifique-se de que o OpenMPI está disponível no sistema

# Define o PYTHONPATH para incluir o diretório de instalação local do pandas
export PYTHONPATH=/mnt/nfs/home/pquilici/.local/lib/python3.9/site-packages:$PYTHONPATH

# Muda para o diretório de trabalho
cd /home/pquilici/gitworkspace/split_offdb

# Executa o script Python com MPI e redireciona a saída para um arquivo de log
mpiexec -n 40 python3 main.py > logs/script.log 2>&1
