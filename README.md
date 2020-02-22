* Sirene Interactive Exploration

* 3 components:
    * HDFS driver
    * Spark Worker
    * Dashboard Server

* Workflow :
    * 1 - HDFS driver download new files from INSEE and store them to local HDFS
    * 2 - Spark Worker take files from local hdfs and execute jobs :
        * 2.1 : Merge Sirene datasets, producing two files stored to local hdfs:
            - 