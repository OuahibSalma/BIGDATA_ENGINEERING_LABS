# Lab1 — API HDFS (Java seulement)

Build:
```bash
cd Lab1/hdfs_api_java
mvn -q -DskipTests package
```

Exemples:
```bash
hadoop jar target/hdfs_api_java-1.0-SNAPSHOT.jar edu.ensias.hadoop.hdfslab.HadoopFileStatus /user/root/input purchases.txt achats.txt
hadoop jar target/hdfs_api_java-1.0-SNAPSHOT.jar edu.ensias.hadoop.hdfslab.ReadHDFS /user/root/input/achats.txt
hadoop jar target/hdfs_api_java-1.0-SNAPSHOT.jar edu.ensias.hadoop.hdfslab.HDFSWrite /user/root/input/bonjour.txt "Salut"
```
