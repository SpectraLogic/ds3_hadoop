# DS3 Hadoop File Migrator

---

A Hadoop Map/Reduce application that migrates data between a Hadoop Cluster and a DS3 appliance.

## Installing

To install the latest ds3_hadoop code either download the latest release jar file from the [Releases](../../releases) page or clone the repository and run `./gradlew install` to install the sdk into your local maven repository.

**NOTE:** In order to successfully build ds3_hadoop with gradle the [java_ds3_sdk](https://github.com/SpectraLogic/ds3_java_sdk) must be installed in the local maven repository.

## Running

To use the File Migrator Hadoop must be installed running.  On the name node (or a system that is able to run `hadoop jar` and `hadoop fs` commands) run the jar file with `hadoop jar fileMigrator.jar -h`.  The help menu will display the following:

```

usage: hadoop jar FileMigrator.jar
 -a <accessKeyId>               Access Key ID or have "DS3_ACCESS_KEY" set
                                as an environment variable
 -archives <paths>              comma separated archives to be unarchived
                                on the compute machines.
 -b <bucket>                    The ds3 bucket to copy to
 -c <command>                    What command you want to perform.  Can be:
                                [put, get, buckets, objects, joblist]
 -conf <configuration file>     specify an application configuration file
 -D <property=value>            use value for given property
 -e <endpoint>                  The ds3 endpoint to connect to or have
                                "DS3_ENDPOINT" set as an environment
                                variable.
 -files <paths>                 comma separated files to be copied to the
                                map reduce cluster
 -fs <local|namenode:port>      specify a namenode
 -h                             Print Help Menu
 -i <directory>                 The directory to copy to ds3
 -jt <local|jobtracker:port>    specify a job tracker
 -k <secretKey>                 Secret access key or have "DS3_SECRET_KEY"
                                set as an environment variable
 -libjars <paths>               comma separated jar files to include in
                                the classpath.
 -o <directory>                 The output directory where any errors will
                                be reported
 -p <prefix>                       Specify a prefix to restore a bucket to.
                                This is an optional argument
 -s                             Set if the connection to the ds3 endpoint
                                should be sent via https
 -tokenCacheFile <tokensFile>   name of the file with the tokens


```

The File Migrator application builds on top of the existing Map/Reduce command line interface to add it's own commands.  The only required arguments are `-c <command>`, `-a <accessKeyId>`, `-e <endpoint>`, and `-k <secretKey>`.  The `endpoint`, `accessId`, and `secretKey` can also all be specified with environment variables.


### Example Usage

**Note:** All the following examples assume that you have set the `accessId`, `secretKey`, and `endpoint` as environment variables.

List All Buckets

`hadoop jar fileMigrator.jar -c buckets`

Output:

```

+-------------+--------------------------+
| Bucket Name |       Creation Date      |
+-------------+--------------------------+
| bulkBucket  | 2014-02-28T11:26:51.000Z |
| test        | 2014-03-10T15:44:28.000Z |
| books       | 2014-01-10T25:43:27.000Z |
+-------------+--------------------------+

```

List All Objects in A Bucket

`hadoop jar fileMigrator.jar -c objects -b books`

Output:

```

+---------------------------+--------+-------+---------------+----------------------------------+
|         File Name         |  Size  | Owner | Last Modified |               ETag               |
+---------------------------+--------+-------+---------------+----------------------------------+
| books/beowulf.txt         | 301063 |  ryan | N/A           | e887d5adf428935460fc9703570a0e43 |
| books/huckfinn.txt        | 610157 |  ryan | N/A           | 4d3916653132f04e1d680161a11ec71c |
| books/taleoftwocities.txt | 792920 |  ryan | N/A           | 5347b336c74d377f6f707449324f88c1 |
+---------------------------+--------+-------+---------------+----------------------------------+

```

Copy a Directory to DS3 from Hadoop

`hadoop jar fileMigrator.jar -c put -b newBooks -i books -o result`

The `-i` specifies the directory to copy.  `-o` is used to record any errors that occured when transfering the files.  To verify that the objects have been put successfully list all the objects in the bucket.

Output:

```
----- Generating File List -----
Verify bucket exists.
got buckets back: {{owner:: id: ryan displayName: ryan},
{buckets:: [name: bulkBucket creationDate: 2014-02-28T11:26:51.000Z,
name: books creationDate: 2014-03-10T15:05:24.000Z]}}
----- Priming DS3 -----
Files to perform bulk put for: [/books/beowulf.txt:301063, /books/huckfinn.txt:610157, /books/taleoftwocities.txt:792920]
Hadoop tmp dir: /home/hduser/tmp
FileList: /home/hduser/tmp/migrator2521539637983000952dat
----- Starting job -----
...

14/03/10 15:44:33 INFO mapreduce.Job:  map 100% reduce 100%
14/03/10 15:44:33 INFO mapreduce.Job: Job job_local2097298783_0001 completed successfully
14/03/10 15:44:33 INFO mapreduce.Job: Counters: 32
        File System Counters
                FILE: Number of bytes read=45088088
                FILE: Number of bytes written=45729234
                FILE: Number of read operations=0
                FILE: Number of large read operations=0
                FILE: Number of write operations=0
                HDFS: Number of bytes read=3408412
                HDFS: Number of bytes written=132
                HDFS: Number of read operations=29
                HDFS: Number of large read operations=0
                HDFS: Number of write operations=6
        Map-Reduce Framework
                Map input records=3
                Map output records=0
                Map output bytes=0
                Map output materialized bytes=6
                Input split bytes=121
                Combine input records=0
                Combine output records=0
                Reduce input groups=0
                Reduce shuffle bytes=0
                Reduce input records=0
                Reduce output records=0
                Spilled Records=0
                Shuffled Maps =0
                Failed Shuffles=0
                Merged Map outputs=0
                GC time elapsed (ms)=0
                CPU time spent (ms)=0
                Physical memory (bytes) snapshot=0
                Virtual memory (bytes) snapshot=0
                Total committed heap usage (bytes)=404750336
        File Input Format Counters
                Bytes Read=66
        File Output Format Counters
                Bytes Written=0
----- Finished Job -----

```

Copy a Bucket from DS3 and store in Hadoop

`hadoop jar fileMigrator.jar -c get -b newBooks -o results`

The files in the bucket will be restored to the current working directory in hadoop.  To store the files in an alternative directory location the `-p <prefix>` argument can be used. `-o` is used for reporting errors.

Output:

```

14/03/10 15:46:33 INFO jvm.JvmMetrics: Initializing JVM Metrics with processName=JobTracker, sessionId=
14/03/10 15:46:33 INFO jvm.JvmMetrics: Cannot initialize JVM Metrics with processName=JobTracker, sessionId= - already initialized
14/03/10 15:46:33 WARN util.NativeCodeLoader: Unable to load native-hadoop library for your platform... using builtin-java classes where applicable
14/03/10 15:46:33 WARN snappy.LoadSnappy: Snappy native library not loaded
14/03/10 15:46:33 INFO mapred.FileInputFormat: Total input paths to process : 1
14/03/10 15:46:33 INFO mapreduce.JobSubmitter: number of splits:1
...
Processing file: books/beowulf.txt
Writing to file: /user/hduser/novels/books/beowulf.txt
Processing file: books/huckfinn.txt
Writing to file: /user/hduser/novels/books/huckfinn.txt
Processing file: books/taleoftwocities.txt
Writing to file: /user/hduser/novels/books/taleoftwocities.txt
14/03/10 15:46:34 INFO mapred.LocalJobRunner:
14/03/10 15:46:34 INFO mapred.MapTask: Starting flush of map output
14/03/10 15:46:34 INFO mapred.Task: Task:attempt_local627526893_0001_m_000000_0 is done. And is in the process of committing
14/03/10 15:46:34 INFO mapred.LocalJobRunner: hdfs://localhost:54310/home/hduser/tmp/migrator1636871833859423139dat:0+63
14/03/10 15:46:34 INFO mapred.Task: Task 'attempt_local627526893_0001_m_000000_0' done.
14/03/10 15:46:34 INFO mapred.LocalJobRunner: Finishing task: attempt_local627526893_0001_m_000000_0
14/03/10 15:46:34 INFO mapred.LocalJobRunner: Map task executor complete.
14/03/10 15:46:34 INFO mapred.Task:  Using ResourceCalculatorProcessTree : [ ]
14/03/10 15:46:34 INFO mapred.Merger: Merging 1 sorted segments
14/03/10 15:46:34 INFO mapred.Merger: Down to the last merge-pass, with 0 segments left of total size: 0 bytes
14/03/10 15:46:34 INFO mapred.LocalJobRunner:
14/03/10 15:46:34 INFO mapred.Task: Task:attempt_local627526893_0001_r_000000_0 is done. And is in the process of committing
14/03/10 15:46:34 INFO mapred.LocalJobRunner:
14/03/10 15:46:34 INFO mapred.Task: Task attempt_local627526893_0001_r_000000_0 is allowed to commit now
14/03/10 15:46:35 INFO output.FileOutputCommitter: Saved output of task 'attempt_local627526893_0001_r_000000_0' to hdfs://localhost:54310/user/hduser/results/_temporary/0/task_local627526893_0001_r_000000
14/03/10 15:46:35 INFO mapred.LocalJobRunner: reduce > reduce
14/03/10 15:46:35 INFO mapred.Task: Task 'attempt_local627526893_0001_r_000000_0' done.
14/03/10 15:46:35 INFO mapreduce.Job: Job job_local627526893_0001 running in uber mode : false
14/03/10 15:46:35 INFO mapreduce.Job:  map 100% reduce 100%
14/03/10 15:46:35 INFO mapreduce.Job: Job job_local627526893_0001 completed successfully
14/03/10 15:46:35 INFO mapreduce.Job: Counters: 32
        File System Counters
                FILE: Number of bytes read=45088082
                FILE: Number of bytes written=45728276
                FILE: Number of read operations=0
                FILE: Number of large read operations=0
                FILE: Number of write operations=0
                HDFS: Number of bytes read=126
                HDFS: Number of bytes written=3408406
                HDFS: Number of read operations=19
                HDFS: Number of large read operations=0
                HDFS: Number of write operations=12
        Map-Reduce Framework
                Map input records=3
                Map output records=0
                Map output bytes=0
                Map output materialized bytes=6
                Input split bytes=121
                Combine input records=0
                Combine output records=0
                Reduce input groups=0
                Reduce shuffle bytes=0
                Reduce input records=0
                Reduce output records=0
                Spilled Records=0
                Shuffled Maps =0
                Failed Shuffles=0
                Merged Map outputs=0
                GC time elapsed (ms)=0
                CPU time spent (ms)=0
                Physical memory (bytes) snapshot=0
                Virtual memory (bytes) snapshot=0
                Total committed heap usage (bytes)=403701760
        File Input Format Counters
                Bytes Read=63
        File Output Format Counters
                Bytes Written=0

```

### Example RC File

```bash

export DS3_ACCESS_KEY="access_key"
export DS3_SECRET_KEY="secret_key"
export DS3_ENDPOINT="hostname:8080"

```

To use the rc file use `source my_rc_file.rc` which will export all of the environment variables into the current bash shell and will be picked up by the CLI.
