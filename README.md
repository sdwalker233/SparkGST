# SparkGST
Parallel construction of generalized suffix tree in Spark.

### Compile
```
git clone https://github.com/shad0w-walker233/SparkGST.git
cd SparkGST
sbt package
```
### Execute
```
${SPARK_HOME/bin}/spark-submit \
--master <spark cluster master uri> \
--class GST.Main \
--executor-memory 15G \
--driver-memory 15G \
--executor-cores 4 \
<jar file path> \
hdfs://input path \
hdfs://output path \
```

### Algorithm
1. Read all the files under the input path.
2. Pretreatment: Determine which substring can be a key.
3. Map Stage: For each suffix, generate a node linking to root node with the key of the first several characters which can be a key.
4. Reduce Stage: Combine trees to generate the subtree of the GST by key.
5. Recursive traversal and output the information of leaf nodes.
