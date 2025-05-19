out=$(date '+%d%m%Y_%H%M%S')
rm -r experiments/$out/
mkdir -p experiments/$out/ls

INPUT="in/"
JAR_JOIN="similarity-jobs-1.0.jar"
#hdfs dfs -rm -r randoms

# 1.a ComputeHistogram
H_OUT="histogram/"
hdfs dfs -rm -r $H_OUT
hadoop jar $JAR_JOIN jobs.mrsf.ComputeHistogram -conf "conf_mrfv.xml" $INPUT $H_OUT $1
if [ $? -ne 0 ]; then
   exit 1
fi
hdfs dfs -ls -h $H_OUT >> experiments/$out/ls/histogram

# 1.b Distribute
H_DISTRIBUTED="h-distributed/"
hdfs dfs -rm -r $H_DISTRIBUTED
hadoop jar $JAR_JOIN jobs.mrsf.DistributeHistogram -conf "conf_mrfv.xml" $H_OUT $H_DISTRIBUTED $3
if [ $? -ne 0 ]; then
   exit 1
fi
hdfs dfs -ls -h $H_DISTRIBUTED >> experiments/$out/ls/h-distributed

# --- CLEAN
hdfs dfs -rm -r $H_OUT

# 2.a Filter
FILTER_OUTPUT="filtered/"
hdfs dfs -rm -r $FILTER_OUTPUT
hadoop jar $JAR_JOIN jobs.mrsf.Filter -conf "conf_mrfv.xml" $INPUT $FILTER_OUTPUT $H_DISTRIBUTED $2
if [ $? -ne 0 ]; then
   exit 1
fi
hdfs dfs -ls -h $FILTER_OUTPUT >> experiments/$out/ls/filter

# --- CLEAN
hdfs dfs -rm -r $H_DISTRIBUTED

# 2.b Distribute filtered
FILTER_DISTRIBUTED="filtered-distributed/"
hdfs dfs -rm -r $FILTER_DISTRIBUTED
hadoop jar $JAR_JOIN jobs.mrsf.Distribute -conf "conf_mrfv.xml" $FILTER_OUTPUT $FILTER_DISTRIBUTED $3
if [ $? -ne 0 ]; then
   exit 1
fi
hdfs dfs -ls -h $FILTER_DISTRIBUTED >> experiments/$out/ls/f-distributed

# --- CLEAN
hdfs dfs -rm -r $FILTER_OUTPUT

# 3. Verify
VERIFY_OUTPUT="verified/"
hdfs dfs -rm -r $VERIFY_OUTPUT
hadoop jar $JAR_JOIN jobs.mrsf.Verify -conf "conf_mrfv.xml" $INPUT $VERIFY_OUTPUT $FILTER_DISTRIBUTED $4
if [ $? -ne 0 ]; then
   exit 1
fi
hdfs dfs -ls -h $VERIFY_OUTPUT >> experiments/$out/ls/join

# --- CLEAN
hdfs dfs -rm -r $FILTER_DISTRIBUTED

# --- RESULTS
# mkdir -p experiments/$out/results
# hdfs dfs -get $VERIFY_OUTPUT experiments/$out/results
hdfs dfs -rm -r $VERIFY_OUTPUT
hdfs dfs -get /exp/* experiments/$out/
hdfs dfs -rm -r randoms
hdfs dfs -rm /exp/*
