package com.spectralogic.hadoop.commands;

import com.spectralogic.ds3client.Ds3Client;
import com.spectralogic.ds3client.Ds3ClientBuilder;
import com.spectralogic.ds3client.models.*;

import com.spectralogic.hadoop.Arguments;
import com.spectralogic.hadoop.util.PathUtils;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.security.SignatureException;
import java.util.ArrayList;
import java.util.List;

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
            } catch (final IOException e) {
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
            final Path filePath = new Path(PathUtils.ensureStartsWithSlash(fileName));

            System.out.println("Processing file: " + fileName);


            try {
                final InputStream getStream = client.getObject(bucketName, fileName);
                final FSDataOutputStream hdfsStream = hadoopFs.create(filePath);

                IOUtils.copy(getStream, hdfsStream);
                getStream.close();
                hdfsStream.close();

            } catch (final SignatureException e) {
                System.out.println("Failed to compute DS3 signature");
                e.printStackTrace();
                throw new IOException(e);
            }
        }
    }

    public GetCommand(final Arguments args) throws IOException {
        super(args);
    }

    @Override
    public void init(JobConf conf) {
        conf.setOutputKeyClass(Text.class);
        conf.setOutputValueClass(LongWritable.class);

        conf.setMapperClass(BulkGet.class);

        conf.setInputFormat(TextInputFormat.class);
        conf.setOutputFormat(TextOutputFormat.class);
    }

    @Override
    public Boolean call() throws Exception {

        // ------------- Get file list from DS3 -------------
        final ListBucketResult fileList = getDs3Client().listBucket(getBucket());

        System.out.println("Files to prime for bulk get");
        System.out.println(fileList);

        // prime ds3
        final MasterObjectList result = getDs3Client().bulkGet(getBucket(), convertToList(fileList));

        final File tempFile = writeToTemp(result);

        final String fileListFile = PathUtils.join(getConf().get("hadoop.tmp.dir"), tempFile.getName());

        System.out.println("FileList: " + fileListFile);
        getHdfs().copyFromLocalFile(new Path(tempFile.toString()), new Path(getConf().get("hadoop.tmp.dir")));

        FileInputFormat.setInputPaths(getConf(), fileListFile);
        FileOutputFormat.setOutputPath(getConf(), getOutputDirectory());

        final RunningJob runningJob = JobClient.runJob(getConf());
        runningJob.waitForCompletion();

        return true;
    }

    private List<Ds3Object> convertToList(final ListBucketResult bucketList) {
        final List<Contents> contentList = bucketList.getContentsList();
        final List<Ds3Object> objectList = new ArrayList<Ds3Object>();

        for(final Contents contents: contentList) {
            objectList.add(new Ds3Object(contents.getKey(), contents.getSize()));
        }
        return objectList;
    }
}
