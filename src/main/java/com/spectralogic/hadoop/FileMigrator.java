package com.spectralogic.hadoop;

import com.spectralogic.ds3client.Ds3Client;
import com.spectralogic.ds3client.Ds3ClientBuilder;
import com.spectralogic.ds3client.FailedRequestException;
import com.spectralogic.ds3client.models.*;
import com.spectralogic.ds3client.models.Objects;
import com.spectralogic.ds3client.serializer.XmlProcessingException;
import org.apache.commons.cli.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.GenericOptionsParser;

import java.io.*;
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

    public FileMigrator(final Arguments arguments) throws IOException {
        final Ds3ClientBuilder builder = new Ds3ClientBuilder("192.168.56.101",new Credentials("cnlhbg==","Secureryan"));
        ds3Client = builder.withHttpSecure(false).withPort(8080).build();

        bucket = arguments.getBucket();
        inputDirectory = new Path(arguments.getSrcDir());
        outputDirectory = new Path(arguments.getDestDir());

        hdfs = FileSystem.get(new Configuration());

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
    }

    public void run() throws IOException, XmlProcessingException, FailedRequestException, SignatureException {

        System.out.println("----- Generating File List -----");

        final List<FileStatus> fileList = getFileList(inputDirectory);
        final List<Ds3Object> objectList = convertFileStatusList(fileList);

        final MasterObjectList masterObjectList = ds3Client.bulkPut("/"+ bucket +"/", objectList);

        final File tempFile = File.createTempFile("migrator","dat");
        final PrintWriter writer = new PrintWriter(new FileWriter(tempFile));
        for(final Objects objects: masterObjectList.getObjects()) {
            for(final Ds3Object object: objects.getObject()) {
                writer.println(object.getName());
            }
        }
        writer.close();

        System.out.println("----- Priming DS3 -----");


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
        obj.setName(fileStatus.getPath().toString());
        obj.setSize(fileStatus.getLen());
        return obj;
    }

    private static Arguments processArgs(final String args[]) throws IOException, MissingOptionException {
        final Arguments arguments = new Arguments();
        final Options options = arguments.getOptions();
        final GenericOptionsParser optParser = new GenericOptionsParser(new Configuration(), options, args);

        arguments.processCommandLine(optParser.getCommandLine());

        return arguments;
    }

    public static void main(final String args[]) throws IOException, XmlProcessingException, FailedRequestException, SignatureException, MissingOptionException {
        final Arguments arguments = processArgs(args);
        final FileMigrator migrator = new FileMigrator(arguments);

        //migrator.run();
    }


}
