INPUT="input/"

$HADOOP_HOME/bin/hdfs dfs -rm -r $INPUT
$HADOOP_HOME/bin/hadoop fs -mkdir -p $INPUT
$HADOOP_HOME/bin/hdfs dfs -put $1 $INPUT
