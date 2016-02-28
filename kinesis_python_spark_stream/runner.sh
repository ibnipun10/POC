#!/bin/sh

_baseFolder="/home/ubuntu"
_sparkMaster="spark://noi-sams-spark-m:7077"
_sqlServer="noi-sams-dev"
_analyticsFolder="POC"
_sqlUsername="cstest"
_sqlPassword="Monotype123#"
_sqlDB="SAMSAnalytics"
_executorMemory=500M
_driverMemory=500M

_logDir="$_baseFolder/ApacheSpark/logs"
_logFile="$_logDir/"$fullDate".log"
_sparkDir="$_baseFolder/ApacheSpark/spark-1.6.0-bin-hadoop2.4"
_redShiftPackage="com.databricks:spark-redshift_2.10:0.6.0"
_executableFile="kinesis_python_spark_stream/kinesisStream.py"

#create log directory if not eixts
mkdir -p $_logDir

#$_sparkDir/sbin/start-all.sh >> $_logFile

#Eun the jobs
$_sparkDir/bin/spark-submit  --packages $_redShiftPackage $_analyticsFolder/$_executableFile >> $_logFile



