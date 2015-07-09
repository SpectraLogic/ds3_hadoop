Spectra S3 Hadoop CLI
==========

**Note:** The CLI is currently under active development and the functionaly and behavior is subject to change and should be considered unstable

The Spectra S3 Hadoop CLI provides a means to export/import data from a hadoop cluster to a Black Pearl.  The current version of the CLI runs using the `hadoop jar` command and must be ran on the name node for the cluster.  There is work planned to add remote support.

## Build

To build the Spectra S3 Hadoop CLI clone the git repository `git clone https://github.com/SpectraLogic/ds3_hadoop.git` then run, `./gradlew clean ds3-hadoop-cli:jar`.  The gradle command will build the cli jar file that is ran with the `hadoop jar` command.  The jar file created is `./ds3-hadoop-cli/build/libs/ds3-hadoop-cli-0.8.2.jar`

## Run

As mentioned to run the Spectra S3 Hadoop CLI, copy `ds3-hadoop-cli-0.8.2.jar` to the name node server in the Hadoop Cluster, then using the `hadoop` command run: `hadoop jar ds3-hadoop-cli-0.8.2.jar -h` which will print out the help menu.

Like the [ds3_java_cli](https://github.com/SpectraLogic/ds3_java_cli#linux-configuration) you can create environment variables to configure the Black Pearl endpoint and credentials.

## Commands

The Spectra S3 Hadoop CLI has support for the following commands:

* `buckets`: List all the buckets on the Black Pearl
* `objects`: List all the objects in a bucket on the Black Pearl
* `put`: Copies a directory from the Hadoop Cluster to Black Pearl
 
There is planned support for the following additional commands:
* `get`: Copies a directory from Black Pearl to the Hadoop Cluster
* `jobs`: List all the currently pending Black Pearl Jobs

## Example Commands

`buckets`: `hadoop jar ds3-hadoop-cli-0.8.2.jar --http -c buckets`

`objects`: `hadoop jar ds3-hadoop-cli-0.8.2.jar --http -c objects -b my_bucket_name`

`put`: `hadoop jar ds3-hadoop-cli-0.8.2.jar --http -c put -b my_put_bucket -d dataDir`

## Known issues

Currently the Spectra S3 Hadoop CLI will on startup print:

```
SLF4J: Failed to load class "org.slf4j.impl.StaticLoggerBinder".
SLF4J: Defaulting to no-operation (NOP) logger implementation
SLF4J: See http://www.slf4j.org/codes.html#StaticLoggerBinder for further details.
```

These messages can safely be ignored and will be removed in subsequent versions.

The `--http` or `--insecure` flag must always be used.  `--http` will tell the mappers to use vanila http while the `--insecure` flag will tell the mappers to use https, but not verify the ssl certificates.
There is not currently support for configuring the proxy settings for the connections between the mappers and Black Pearl.
The `put` command does not currently display any information on the status of the job.  We are researching what will be the best indicator that progress is being made.
