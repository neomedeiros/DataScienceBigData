/*
* PIG script para apresentar o valor de empenho das secretarias da prefeitura de Curitiba no Ano de 2018
* Trabalho de Apache PIG - Pós Graduação Data Science e Big Data - Universidade Positivo
* Aluno: Leandro Medeiros
*/

despesas = LOAD '/user/hadoop/dados/curitiba/despesas.csv' USING PigStorage(';') as (ano: chararray, secretaria:chararray, empenho:chararray);
despesas2 = FILTER despesas BY ano !='ANO_EMPENHO' AND ano !='-----------';
despesas3 = FOREACH despesas2 GENERATE ano,secretaria,(float)REPLACE(empenho,',','.') AS valorempenho;
despesas4 = GROUP despesas3 BY secretaria;
despesas5 = FOREACH despesas4 GENERATE group,ROUND(SUM(despesas3.valorempenho));
DUMP despesas5;
