package com.spectralogic.hadoop.hdfs.sample;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.aggregate.ValueAggregatorCombiner;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.map.TokenCounterMapper;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.log4j.BasicConfigurator;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.log4j.spi.RootLogger;

import java.io.IOException;
import java.io.InputStream;
import java.security.PrivilegedExceptionAction;
import java.util.ArrayList;
import java.util.List;

public class SubmitJob {

    public static void main(final String[] args) throws IOException, InterruptedException {

        BasicConfigurator.configure();

        final RootLogger logger = (RootLogger) Logger.getRootLogger();
        logger.setLevel(Level.DEBUG);


        final Configuration conf = new Configuration();
        final UserGroupInformation usgi = UserGroupInformation.createRemoteUser("root");

        usgi.doAs(new PrivilegedExceptionAction<Object>() {
            @Override
            public Object run() throws Exception {
                conf.set("fs.default.name", "hdfs://172.17.0.3:9000");
                conf.set("mapred.job.tracker", "172.17.0.3:8033");
                conf.set("mapreduce.framework.name", "yarn");
                try (final FileSystem hdfs = FileSystem.get(conf)) {

                    final Path resultDir = new Path("result");
                    if (hdfs.exists(resultDir)) {
                        System.out.println("Cleaning up the result directory.");
                        hdfs.delete(resultDir, true);
                    }

                    System.out.printf("Total Used Hdfs Storage: %d\n", hdfs.getStatus().getUsed());
                    final List<Path> objects = populateTestData(hdfs);

                    Job job = Job.getInstance(conf);
                    job.setJarByClass(SubmitJob.class);

                    // Specify various job-specific parameters
                    job.setJobName("myjob");

                    org.apache.hadoop.mapreduce.lib.input.TextInputFormat.addInputPath(job, hdfs.makeQualified(objects.get(0)));
                    FileOutputFormat.setOutputPath(job, new Path("result"));

                    job.setMapperClass(TokenCounterMapper.class);
                    //job.setReducerClass(ValueAggregatorCombiner.class);

                    // Submit the job, then poll for progress until the job is complete
                    job.waitForCompletion(true);

                }
                return null;
            }
        });
    }

    private static List<Path> populateTestData(final FileSystem hdfs) throws IOException {

        final String[] resources = new String[]{"books/beowulf.txt", "books/sherlock_holmes.txt", "books/tale_of_two_cities.txt", "books/ulysses.txt"};
        final List<Path> objects = new ArrayList<>();

        for (final String resourceName : resources) {

            System.out.println("Processing: " + resourceName);
            final Path path = new Path("/user/root", resourceName);

            try (final InputStream inputStream = SubmitJob.class.getClassLoader().getResourceAsStream(resourceName);
                 final FSDataOutputStream outputStream = hdfs.create(path, true)) {

                IOUtils.copy(inputStream, outputStream);
                objects.add(path);
            }
        }

        return objects;
    }
}
