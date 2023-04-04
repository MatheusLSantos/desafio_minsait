#!/bin/bash
# Criando o agrupamento de pastas do DATALAKE
pastas=("" "raw" "silver" "gold") #pastas que serão criadas
for pasta in "${pastas[@]}"
do
	hdfs dfs -mkdir /datalake/"$pasta"
done
arquivos=($(cd /input/desafio_curso/raw && ls))
for arquivo in "${arquivos[@]}"
do
	hdfs dfs -copyFromLocal /input/desafio_curso/raw/$arquivo /datalake/raw
done
