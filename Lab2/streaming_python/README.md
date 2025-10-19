# Lab2 — MapReduce (Python, Hadoop Streaming)

Test local:
```bash
cat alice.txt | python3 mapper.py | sort -k1,1 | python3 reducer.py | head
```

Run sur cluster:
```bash
hadoop jar /opt/hadoop-3.2.1/share/hadoop/tools/lib/hadoop-streaming-3.2.1.jar   -files mapper.py,reducer.py -mapper "python3 mapper.py" -reducer "python3 reducer.py"   -input /user/root/web_input/alice.txt -output /user/root/out_streaming
hdfs dfs -cat /user/root/out_streaming/part-* | head
```
