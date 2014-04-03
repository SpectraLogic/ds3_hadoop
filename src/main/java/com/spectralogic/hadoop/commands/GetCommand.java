package com.spectralogic.hadoop.commands;

import com.google.common.collect.Lists;
import com.spectralogic.ds3client.Ds3Client;
import com.spectralogic.ds3client.commands.BulkGetRequest;
import com.spectralogic.ds3client.commands.GetBucketRequest;
import com.spectralogic.ds3client.commands.GetObjectRequest;
import com.spectralogic.ds3client.models.*;
import com.spectralogic.hadoop.Arguments;
import com.spectralogic.hadoop.util.ListUtils;
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
        private String prefix;

        @Override
        public void configure(final JobConf conf) {
            final Ds3Client.Builder builder = Ds3Client.builder(conf.get("endpoint"), new Credentials(conf.get("accessKeyId"), conf.get("secretKey")));
            client = builder.withHttpSecure(Boolean.valueOf(conf.get("secure"))).build();
            try {
                hadoopFs = FileSystem.get(new Configuration());
            } catch (final IOException e) {
                e.printStackTrace();
                hadoopFs = null;
            }
            bucketName = conf.get("bucket");
            prefix = conf.get("prefix");
        }

        @Override
        public void map(LongWritable longWritable, Text value, OutputCollector<Text, LongWritable> textLongWritableOutputCollector, Reporter reporter) throws IOException {
            if(hadoopFs == null) {
                throw new IOException("Could not connect to the hadoop fs.");
            }
            final String rootPathName = PathUtils.join(PathUtils.getWorkingDirPath(hadoopFs), prefix);
            final String fileEndPath = value.toString();
            final String fileName = PathUtils.join(rootPathName, fileEndPath);

            final Path ds3FilePath = new Path(PathUtils.ensureStartsWithSlash(fileName));

            System.out.println("Processing file: " + fileEndPath);
            System.out.println("Writing to file: " + fileName);


            try(final InputStream getStream = client.getObject(new GetObjectRequest(bucketName, fileEndPath)).getContent();
                final FSDataOutputStream hdfsStream = hadoopFs.create(ds3FilePath)) {

                IOUtils.copy(getStream, hdfsStream);

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
    public void init(final JobConf conf) {
        conf.setOutputKeyClass(Text.class);
        conf.setOutputValueClass(LongWritable.class);

        conf.setMapperClass(BulkGet.class);

        conf.setInputFormat(TextInputFormat.class);
        conf.setOutputFormat(TextOutputFormat.class);
    }

    @Override
    public Boolean call() throws Exception {

        // ------------- Get file list from DS3 -------------
        final ListBucketResult fileList = getDs3Client().getBucket(new GetBucketRequest(getBucket())).getResult();
        final List<Ds3Object> objects = convertToList(fileList);

        // prime ds3
        final MasterObjectList result = getDs3Client().bulkGet(new BulkGetRequest(getBucket(), Lists.newArrayList(ListUtils.filterDirectories(objects)))).getResult();
        final File tempFile = writeToTemp(result);

        final String fileListFile = PathUtils.join(getConf().get("hadoop.tmp.dir"), tempFile.getName());

        getHdfs().copyFromLocalFile(new Path(tempFile.toString()), new Path(getConf().get("hadoop.tmp.dir")));

        FileInputFormat.setInputPaths(getConf(), fileListFile);
        FileOutputFormat.setOutputPath(getConf(), getOutputDirectory());

        final RunningJob runningJob = JobClient.runJob(getConf());
        runningJob.waitForCompletion();

        return true;
    }

    private List<Ds3Object> convertToList(final ListBucketResult bucketList) {
        final List<Contents> contentList = bucketList.getContentsList();
        final List<Ds3Object> objectList = new ArrayList<>();

        for(final Contents contents: contentList) {
            objectList.add(new Ds3Object(contents.getKey(), contents.getSize()));
        }
        return objectList;
    }
}
