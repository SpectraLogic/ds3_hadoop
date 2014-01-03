package com.spectralogic.hadoop.commands;

import com.spectralogic.ds3client.Ds3Client;
import com.spectralogic.ds3client.Ds3ClientBuilder;
import com.spectralogic.ds3client.models.Credentials;
import com.spectralogic.hadoop.Arguments;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;

import java.io.IOException;

public class GetCommand extends AbstractCommand {

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

    public GetCommand(final Arguments args) throws IOException {
        super(args);
    }

    @Override
    public void init(JobConf conf) {

    }

    @Override
    public Boolean call() throws Exception {
        return null;
    }
}
