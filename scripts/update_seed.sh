SEEDS="seeds/"
LSH=${SEEDS}lsh
ESTIMATOR=${SEEDS}estimator
LSH_REDUCER=${SEEDS}sampling

$HADOOP_HOME/bin/hdfs dfs -rm -r $SEEDS
$HADOOP_HOME/bin/hadoop fs -mkdir -p $SEEDS

IDLSH=1 #$(( $RANDOM % 6 + 1));
IDESTIMATOR=2 #$(( $RANDOM % 6 + 1));
IDFILTER=3 #$(( $RANDOM % 6 + 1));

$HADOOP_HOME/bin/hdfs dfs -put seeds/$IDLSH $LSH
$HADOOP_HOME/bin/hdfs dfs -put seeds/$IDESTIMATOR $ESTIMATOR
$HADOOP_HOME/bin/hdfs dfs -put seeds/$IDFILTER $LSH_REDUCER
