#!/bin/bash
# Criando o agrupamento de pastas do DATALAKE
pastas=("" "raw" "silver" "gold") #pastas que serão criadas
for pasta in "${pastas[@]}"
do
	hdfs dfs -mkdir /datalake/"$pasta"
done
