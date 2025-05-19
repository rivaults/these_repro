INPUT="input/"
TMP="tmp/"
OUTPUT="in/"

hdfs dfs -rm -r $TMP

hadoop jar similarity-tokenizer-1.0.jar jobs.Uniq -conf "conf_tokenizer.xml" $INPUT $TMP $1
if [ $? -ne 0 ]; then
   exit 1
fi

hdfs dfs -rm -r $OUTPUT
hadoop jar similarity-tokenizer-1.0.jar jobs.Shuffle -conf "conf_tokenizer.xml" $TMP $OUTPUT $1
if [ $? -ne 0 ]; then
   exit 1
fi

hdfs dfs -rm -r $TMP