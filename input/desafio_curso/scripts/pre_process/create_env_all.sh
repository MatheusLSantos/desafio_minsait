#!/bin/bash

# Criação das pastas

DADOS=("VENDAS" "CLIENTES" "ENDERECO" "REGIAO" "DIVISAO")

for i in "${DADOS[@]}"
do
	echo "$i"
    cd ../../raw/
    hdfs dfs -mkdir /datalake/raw/$i
    hdfs dfs -chmod 777 /datalake/raw/$i
    hdfs dfs -copyFromLocal -n $i.csv /datalake/raw/$i
    beeline -u jdbc:hive2://localhost:10000 -f ../../scripts/hql/create_table_$i.hql 
done
