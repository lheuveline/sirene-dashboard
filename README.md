* Sirene Interactive Exploration

* 3 components:
    * HDFS driver
    * Spark Worker
    * Dashboard Server

* Workflow :
    * 1 - bash script from hdfs folder download new files from INSEE and store them to local HDFS. Make sure hadoop is in PATH.
    * 2 - Spark Worker take files from local hdfs and execute 2 jobs :
        * 2.1 : Merge Sirene datasets, producing two files stored to local hdfs:
            - "unite_etab_joined" : Complete raw merge
            - "activity_category_postal_codes" : smaller subset adding coordinates when found in INSEE geolocation file
        * 2.2 : Extract counts for interactive exploration to dashboard data folder.
    * 3 - Panel is used to display extracted data interatively.