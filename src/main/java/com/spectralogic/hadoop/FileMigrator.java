package com.spectralogic.hadoop;

import com.spectralogic.ds3client.Ds3Client;
import com.spectralogic.ds3client.Ds3ClientBuilder;
import com.spectralogic.ds3client.FailedRequestException;
import com.spectralogic.ds3client.models.*;
import com.spectralogic.ds3client.models.Objects;
import com.spectralogic.ds3client.serializer.XmlProcessingException;
import org.apache.commons.cli.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.GenericOptionsParser;

import java.io.*;
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

    public static class BulkPut extends MapReduceBase implements Mapper<LongWritable, Text, Text, LongWritable> {
        private Text word = new Text();
        private final static LongWritable one = new LongWritable(1);

        private Ds3Client client;
        private FileSystem hadoopFs;
        private String bucketName;

        @Override
        public void configure(final JobConf conf) {
            final Ds3ClientBuilder builder = new Ds3ClientBuilder(conf.get("endpoint"), new Credentials(conf.get("accessKeyId"), conf.get("secretKey")));
            client = builder.withHttpSecure(Boolean.valueOf(conf.get("secure"))).withPort(Integer.parseInt(conf.get("port"))).build();
            try {
                hadoopFs = FileSystem.get(new Configuration());
            } catch (IOException e) {
                e.printStackTrace();
                hadoopFs = null;
            }
            bucketName = conf.get("bucket");
        }

        @Override
        public void map(final LongWritable longWritable, final Text value, final OutputCollector<Text, LongWritable> output, final Reporter reporter) throws IOException {
            if(hadoopFs == null) {
                throw new IOException("Could not connect to the hadoop fs.");
            }
            final String fileName = value.toString();
            final Path filePath = new Path(fileName);
            System.out.println("Processing file: " + fileName);

            final FileStatus fileInfo = hadoopFs.getFileStatus(filePath);
            final FSDataInputStream stream = hadoopFs.open(filePath);

            try {
                client.putObject(bucketName, fileName, fileInfo.getLen(), stream);
            } catch (SignatureException e) {
                System.out.println("Failed to compute DS3 signature");
                throw new IOException(e);
            } finally {
                stream.close();
            }
        }
    }

    public static class BulkGet extends MapReduceBase implements Mapper<LongWritable, Text, Text, LongWritable> {

        private Ds3Client client;
        private FileSystem hadoopFs;
        private String bucketName;

        @Override
        public void configure(final JobConf conf) {
            final Ds3ClientBuilder builder = new Ds3ClientBuilder(conf.get("endpoint"), new Credentials(conf.get("accessKeyId"), conf.get("secretKey")));
            client = builder.withHttpSecure(Boolean.valueOf(conf.get("secure"))).withPort(Integer.parseInt(conf.get("port"))).build();
            try {
                hadoopFs = FileSystem.get(new Configuration());
            } catch (IOException e) {
                e.printStackTrace();
                hadoopFs = null;
            }
            bucketName = conf.get("bucket");
        }

        @Override
        public void map(LongWritable longWritable, Text value, OutputCollector<Text, LongWritable> textLongWritableOutputCollector, Reporter reporter) throws IOException {
            if(hadoopFs == null) {
                throw new IOException("Could not connect to the hadoop fs.");
            }
            final String fileName = value.toString();
            final Path filePath = new Path(fileName);
            System.out.println("Processing file: " + fileName);
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

        conf.setMapperClass(BulkPut.class);

        conf.setInputFormat(TextInputFormat.class);
        conf.setOutputFormat(TextOutputFormat.class);

        conf.set("secure", String.valueOf(arguments.isSecure()));
        conf.set("port", String.valueOf(arguments.getPort()));
        conf.set("bucket", bucket);
        conf.set("accessKeyId", arguments.getAccessKey());
        conf.set("secretKey", arguments.getSecretKey());
        conf.set("endpoint", arguments.getEndpoint());

        hdfs = FileSystem.get(arguments.getConfiguration());
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
        //flush the contents so we can copy them to hdfs
        writer.flush();

        System.out.println("Hadoop tmp dir" + conf.get("hadoop.tmp.dir"));

        final String fileListFile = PathUtils.join(conf.get("hadoop.tmp.dir"), tempFile.getName());

        System.out.println("FileList: " + fileListFile);
        hdfs.copyFromLocalFile(new Path(tempFile.toString()), new Path(conf.get("hadoop.tmp.dir")));

        //Close the file after it's been used to make sure that tmp doesn't clean it up before its been copied to hdfs.
        writer.close();

        FileInputFormat.setInputPaths(conf, fileListFile);
        FileOutputFormat.setOutputPath(conf, outputDirectory);

        System.out.println("----- Starting job -----");


        final RunningJob runningJob = JobClient.runJob(conf);
        runningJob.waitForCompletion();

        System.out.println("----- Finished Job -----");
    }

    /**
     * Generates a list of all the files contained within @param directoryPath
     * @param directoryPath
     * @return
     * @throws IOException
     */
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

    private static Arguments processArgs(final String args[]) throws IOException, MissingOptionException, BadArgumentException {
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

    public static void main(final String args[]) throws IOException, XmlProcessingException, FailedRequestException, SignatureException, MissingOptionException, URISyntaxException, BadArgumentException {
        final Arguments arguments = processArgs(args);
        final FileMigrator migrator = new FileMigrator(arguments);

        migrator.run();
    }

}
