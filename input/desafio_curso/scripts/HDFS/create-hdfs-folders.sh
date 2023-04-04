#!/bin/bash
# Criando o agrupamento de pastas do DATALAKE
pastas=("" "raw" "silver" "gold") #pastas que serão criadas
for pasta in "${pastas[@]}"
do
	hdfs dfs -mkdir /datalake/"$pasta"
done
arquivos=($(cd /input/desafio_curso/raw && ls))
sufix=".csv"
for arquivo in "${arquivos[@]}"
do
	hdfs dfs -mkdir /datalake/raw/${arquivo%$sufix}
	hdfs dfs -copyFromLocal /input/desafio_curso/raw/$arquivo /datalake/raw/${arquivo%$sufix}
done
