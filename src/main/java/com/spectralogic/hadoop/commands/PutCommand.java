package com.spectralogic.hadoop.commands;

import com.spectralogic.ds3client.Ds3Client;
import com.spectralogic.ds3client.Ds3ClientBuilder;

import com.spectralogic.ds3client.commands.BulkPutRequest;
import com.spectralogic.ds3client.commands.PutObjectRequest;
import com.spectralogic.ds3client.models.Credentials;
import com.spectralogic.ds3client.models.Ds3Object;
import com.spectralogic.ds3client.models.MasterObjectList;

import com.spectralogic.ds3client.serializer.XmlProcessingException;
import com.spectralogic.hadoop.Arguments;
import com.spectralogic.hadoop.util.PathUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;

import java.io.File;
import java.io.IOException;
import java.security.SignatureException;
import java.util.List;

public class PutCommand extends AbstractCommand {

    public static class BulkPut extends MapReduceBase implements Mapper<LongWritable, Text, Text, LongWritable> {

        private Ds3Client client;
        private FileSystem hadoopFs;
        private String bucketName;

        @Override
        public void configure(final JobConf conf) {
            final Ds3ClientBuilder builder = new Ds3ClientBuilder(conf.get("endpoint"), new Credentials(conf.get("accessKeyId"), conf.get("secretKey")));
            client = builder.withHttpSecure(Boolean.valueOf(conf.get("secure"))).build();
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

            try (final FSDataInputStream stream = hadoopFs.open(filePath)) {
                client.putObject(new PutObjectRequest(bucketName, fileName, fileInfo.getLen(), stream));
            } catch (SignatureException e) {
                System.out.println("Failed to compute DS3 signature");
                throw new IOException(e);
            }
        }
    }

    public PutCommand(final Arguments args) throws IOException {
        super(args);
    }

    @Override
    public void init(final JobConf conf) {
        conf.setOutputKeyClass(Text.class);
        conf.setOutputValueClass(LongWritable.class);

        conf.setMapperClass(BulkPut.class);

        conf.setInputFormat(TextInputFormat.class);
        conf.setOutputFormat(TextOutputFormat.class);
    }

    @Override
    public Boolean call() throws SignatureException, IOException, XmlProcessingException {
        System.out.println("----- Generating File List -----");

        final List<FileStatus> fileList = getFileList(getInputDirectory());

        final List<Ds3Object> objectList = convertFileStatusList(fileList);

        verifyBucketExists();

        System.out.println("----- Priming DS3 -----");

        System.out.println("Files to perform bulk put for: " + objectList.toString());
        final MasterObjectList masterObjectList = getDs3Client().bulkPut(new BulkPutRequest(getBucket(), objectList)).getResult();

        final File tempFile = writeToTemp(masterObjectList);

        final String hadoopTempDir = getConf().get("hadoop.tmp.dir");
        System.out.println("Hadoop tmp dir: " + hadoopTempDir);

        final String fileListFile = PathUtils.join(hadoopTempDir, tempFile.getName());

        System.out.println("FileList: " + fileListFile);
        getHdfs().copyFromLocalFile(new Path(tempFile.toString()), new Path(fileListFile));

        FileInputFormat.setInputPaths(getConf(), fileListFile);
        FileOutputFormat.setOutputPath(getConf(), getOutputDirectory());

        System.out.println("----- Starting job -----");


        final RunningJob runningJob = JobClient.runJob(getConf());
        runningJob.waitForCompletion();

        System.out.println("----- Finished Job -----");
        return true;
    }


}
