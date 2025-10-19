# Lab2 — MapReduce (Java)

Build:
```bash
cd Lab2/mapreduce_java
mvn -q -DskipTests package
```
Run:
```bash
hadoop jar target/mapreduce_java-1.0-SNAPSHOT.jar edu.ensias.hadoop.mapreducelab.WordCount /user/root/web_input /user/root/out_wc
hdfs dfs -cat /user/root/out_wc/part-r-00000 | head
```
