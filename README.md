# HadoopPBFS
Hadoop Parallel BFS 
1. Edit input file with form:
  A	A,Integer.MAX_VALUE|B,10|D,5
  <node><tab><node>,<distance_from_source>,<adjacent_list>
2. Build jar file from project
3. Moving input file to HDFS by command: hadoop fs -copyFromLocal ./input /input
  with ./input is file from local
        /input is destination name on HDFS
4. Run Hadoop by command: hadoop jar filejar.jar ParallelBFS ./input /output
  ParallelBFS: Driver class
  
