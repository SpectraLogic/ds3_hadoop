package com.spectralogic.hadoop;

import com.spectralogic.ds3client.Ds3Client;
import com.spectralogic.ds3client.Ds3ClientBuilder;
import com.spectralogic.ds3client.FailedRequestException;
import com.spectralogic.ds3client.models.*;
import com.spectralogic.ds3client.models.Objects;
import com.spectralogic.ds3client.serializer.XmlProcessingException;
import org.apache.commons.cli.*;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.GenericOptionsParser;

import java.io.*;
import java.net.URI;
import java.net.URISyntaxException;
import java.security.SignatureException;
import java.util.*;

public class FileMigrator {

    private final FileSystem hdfs;
    private final JobConf conf;
    private final Path inputDirectory;
    private final Path outputDirectory;
    private final Ds3Client ds3Client;
    private final String bucket;

    public static class Map extends MapReduceBase implements Mapper<LongWritable, Text, Text, LongWritable> {
        private Text word = new Text();
        private final static LongWritable one = new LongWritable(1);

        @Override
        public void map(LongWritable longWritable, Text value, OutputCollector<Text, LongWritable> output, Reporter reporter) throws IOException {

            final String line = value.toString();
            final StringTokenizer tokenizer = new StringTokenizer(line);
            while(tokenizer.hasMoreTokens()) {
                word.set(tokenizer.nextToken());
                output.collect(word, one);
            }
        }
    }

    public static class Reduce extends MapReduceBase implements Reducer<Text, LongWritable, Text, LongWritable> {

        @Override
        public void reduce(Text text, Iterator<LongWritable> values, OutputCollector<Text, LongWritable> output, Reporter reporter) throws IOException {
            long sum = 0;
            while(values.hasNext()) {
                sum += values.next().get();
            }

            output.collect(text, new LongWritable(sum));
        }
    }

    public FileMigrator(final Arguments arguments) throws IOException, URISyntaxException {
        final Ds3ClientBuilder builder = new Ds3ClientBuilder(arguments.getEndpoint(), new Credentials(arguments.getAccessKey(), arguments.getSecretKey()));
        ds3Client = builder.withHttpSecure(arguments.isSecure()).withPort(arguments.getPort()).build();

        inputDirectory = new Path(arguments.getSrcDir());
        outputDirectory = new Path(arguments.getDestDir());
        bucket = arguments.getBucket();





        conf = new JobConf(FileMigrator.class);
        conf.setJobName("FileMigrator");
        conf.setOutputKeyClass(Text.class);
        conf.setOutputValueClass(LongWritable.class);

        conf.setMapperClass(Map.class);
        conf.setCombinerClass(Reduce.class);
        conf.setReducerClass(Reduce.class);

        conf.setInputFormat(TextInputFormat.class);
        conf.setOutputFormat(TextOutputFormat.class);

        conf.set("bucket", bucket);
        conf.set("accessKeyId", arguments.getAccessKey());
        conf.set("secretKey", arguments.getSecretKey());
        conf.set("endpoint", arguments.getEndpoint());

        hdfs = FileSystem.get(arguments.getConfiguration());


        //hdfs = FileSystem.get(new URI("hdfs://192.168.56.10:54310"), arguments.getConfiguration());

    }

    public void run() throws IOException, XmlProcessingException, FailedRequestException, SignatureException {

        System.out.println("----- Generating File List -----");

        final List<FileStatus> fileList = getFileList(inputDirectory);
        final List<Ds3Object> objectList = convertFileStatusList(fileList);

        verifyBucketExists();

        System.out.println("----- Priming DS3 -----");

        System.out.println("Files to perform bulk put for: " + objectList.toString());
        final MasterObjectList masterObjectList = ds3Client.bulkPut("/"+ bucket +"/", objectList);

        final File tempFile = File.createTempFile("migrator","dat");
        final PrintWriter writer = new PrintWriter(new FileWriter(tempFile));
        for(final Objects objects: masterObjectList.getObjects()) {
            for(final Ds3Object object: objects.getObject()) {
                writer.println(object.getName());
            }
        }
        writer.close();

        FileInputFormat.setInputPaths(conf, inputDirectory);
        FileOutputFormat.setOutputPath(conf, outputDirectory);


        System.out.println("----- Starting job -----");

        final RunningJob runningJob = JobClient.runJob(conf);
        runningJob.waitForCompletion();


        System.out.println("----- Finished Job -----");
    }


    public List<FileStatus> getFileList(final Path directoryPath) throws IOException {
        final ArrayList<FileStatus> fileList = new ArrayList<FileStatus>();
        final FileStatus[] files = hdfs.listStatus(directoryPath);
        if(files.length != 0) {
            for (final FileStatus file: files) {
                if (file.isDir()) {
                    fileList.addAll(getFileList(file.getPath()));
                }
                else {
                    fileList.add(file);
                }
            }
        }
        return fileList;
    }

    private List<Ds3Object> convertFileStatusList(final List<FileStatus> fileList) {
        final List<Ds3Object> objectList = new ArrayList<Ds3Object>();

        for (final FileStatus file: fileList) {
            objectList.add(fileStatusToDs3Object(file));
        }
        return objectList;
    }

    private Ds3Object fileStatusToDs3Object(final FileStatus fileStatus) {
        final Ds3Object obj = new Ds3Object();
        try {
            obj.setName(PathUtils.stripPath(fileStatus.getPath().toString()));
        } catch (URISyntaxException e) {
            System.err.println("The uri passed in was invalid.  This should not happen.");
        }
        obj.setSize(fileStatus.getLen());
        return obj;
    }

    private static Arguments processArgs(final String args[]) throws IOException, MissingOptionException {
        final Arguments arguments = new Arguments();
        final Options options = arguments.getOptions();
        final GenericOptionsParser optParser = new GenericOptionsParser(arguments.getConfiguration(), options, args);

        arguments.processCommandLine(optParser.getCommandLine());

        return arguments;
    }

    /**
     * Verifies to see if the bucket exists, and if it doesn't, creates it.
      * @throws FailedRequestException
     * @throws SignatureException
     * @throws IOException
     */
    private void verifyBucketExists() throws FailedRequestException, SignatureException, IOException {
        System.out.println("Verify bucket exists.");
        final ListAllMyBucketsResult bucketList = ds3Client.getService();
        System.out.println("got buckets back: " + bucketList.toString());

        final List<Bucket> buckets = bucketList.getBuckets();
        if (buckets == null) {
            ds3Client.createBucket(bucket);
            return;
        }

        for(final Bucket bucketInstance: bucketList.getBuckets()) {
            if(bucketInstance.getName().equals(bucket)) {
                System.out.println("Found bucket.");
                return;
            }
        }
        System.out.println("Didn't find bucket, creating.");
        ds3Client.createBucket(bucket);
    }

    public static void main(final String args[]) throws IOException, XmlProcessingException, FailedRequestException, SignatureException, MissingOptionException, URISyntaxException {
        final Arguments arguments = processArgs(args);
        final FileMigrator migrator = new FileMigrator(arguments);

        migrator.run();
    }

}
