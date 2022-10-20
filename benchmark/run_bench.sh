#!/bin/sh
BENCHMARK_TIMES=$1
BATCH_TABLES=$2
BATCH_ROWS=$3

REPORT_NAME="golang_run_${BENCHMARK_TIMES}"
RESULT_FOLDER="result"
INSERT_TABLE_NUM=10000

echo "====== starting ..."
if [ ! -d ${RESULT_FOLDER} ]
then
    mkdir ${RESULT_FOLDER}
fi
echo "BENCHMARK_TIMES:${BENCHMARK_TIMES}"

clear remaining result report
rm ./${RESULT_FOLDER}/*.md

echo "====== preparing ..."
echo "=== build benchmark code "
rm -f benchmark
go build -o benchmark benchmark.go

echo "=== create database for benchmark "
taos -s 'create database if not exists benchmark'

echo "===== step 1 create tables ..."
taos -s 'drop stable if exists benchmark.stb'
taos -s 'drop stable if exists benchmark.jtb'
taosBenchmark -f ./data/only_create_table_with_normal_tag.json
taosBenchmark -f ./data/only_create_table_with_json_tag.json

echo "===== step 2 insert data ..."
hyperfine -r ${BENCHMARK_TIMES} -L types normal,json -L tables ${INSERT_TABLE_NUM} \
 './benchmark -s insert -t {types} -b {tables}' \
 --time-unit millisecond  \
 --show-output \
 --export-markdown ${RESULT_FOLDER}/${REPORT_NAME}_insert.md \
 --command-name insert_{types}_${INSERT_TABLE_NUM}_tables_${BENCHMARK_TIMES}_times

echo "===== step 3 clean data and create tables ..."
taos -s 'drop stable if exists benchmark.stb'
taos -s 'drop stable if exists benchmark.jtb'
taosBenchmark -f ./data/only_create_table_with_normal_tag.json
taosBenchmark -f ./data/only_create_table_with_json_tag.json

echo "===== step 4 insert data with batch ..."
 hyperfine -r ${BENCHMARK_TIMES} -L rows ${BATCH_ROWS} -L tables ${BATCH_TABLES} \
  -L types normal,json \
 './benchmark -s batch -t {types} -r {rows} -b {tables}' \
 --time-unit millisecond  \
 --show-output \
 --export-markdown ${RESULT_FOLDER}/${REPORT_NAME}_bath.md \
 --command-name batch_{types}_${BATCH_TABLES}_tables_${BENCHMARK_TIMES}_times

echo "===== step 5 query..."
hyperfine -r ${BENCHMARK_TIMES} -L types normal,json \
 './benchmark -s query -t {types}' \
 --time-unit millisecond  \
 --show-output \
 --export-markdown ${RESULT_FOLDER}/${REPORT_NAME}_query.md \
 --command-name query_{types}_${BENCHMARK_TIMES}_times

echo "===== step 6 avg ..."
hyperfine -r ${BENCHMARK_TIMES} -L types normal,json \
 './benchmark -s avg -t {types}' \
 --time-unit millisecond  \
 --show-output \
 --export-markdown ${RESULT_FOLDER}/${REPORT_NAME}_avg.md \
 --command-name avg_{types}_${BENCHMARK_TIMES}_times


echo "| Command | Mean [ms] | Min [ms] | Max [ms] | Relative |">>./${RESULT_FOLDER}/${REPORT_NAME}.md
echo "|:---|---:|---:|---:|---:|">>./${RESULT_FOLDER}/${REPORT_NAME}.md
ls ./${RESULT_FOLDER}/*.md|
while read filename;
do
    sed -n '3,4p' ${filename}>>${RESULT_FOLDER}/${REPORT_NAME}.md
done

echo "=== clean database and binary file ... "
rm -f benchmark
taos -s 'drop database benchmark'

echo "=== benchmark done ... "
echo "=== result file:${RESULT_FOLDER}/${REPORT_NAME}.md "
