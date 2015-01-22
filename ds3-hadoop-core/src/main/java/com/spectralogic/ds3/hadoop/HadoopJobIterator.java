package com.spectralogic.ds3.hadoop;

import com.spectralogic.ds3.hadoop.mappers.BulkPut;
import com.spectralogic.ds3.hadoop.options.WriteOptions;
import com.spectralogic.ds3.hadoop.util.HdfsUtils;
import com.spectralogic.ds3.hadoop.util.PathUtils;
import com.spectralogic.ds3client.Ds3Client;
import com.spectralogic.ds3client.models.bulk.MasterObjectList;
import com.spectralogic.ds3client.models.bulk.Objects;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.*;

import java.io.File;
import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.UUID;

public class HadoopJobIterator implements Iterator<RunningJob> {
    private final ChunkAllocator chunkAllocator;
    private final JobClient jobClient;
    private final UUID jobId;
    private final Configuration conf;
    private final Ds3Client ds3Client;
    private final FileSystem hdfs;
    private final WriteOptions options;
    private final String bucketName;

    HadoopJobIterator(final Ds3Client client, final Configuration conf, final FileSystem hdfs, final WriteOptions options, final String bucketName, final MasterObjectList masterObjectList) throws IOException {
        this.conf = conf;
        this.hdfs = hdfs;
        this.options = options;
        this.bucketName = bucketName;
        this.ds3Client = client;
        this.chunkAllocator = new ChunkAllocator(client, masterObjectList.getObjects());
        this.jobClient = new JobClient(this.conf);
        this.jobId = masterObjectList.getJobId();
    }

    private Exception exception = null;

    public Exception getException() {
        return exception;
    }

    @Override
    public boolean hasNext() {
        return chunkAllocator.hasMoreChunks();
    }

    @Override
    public RunningJob next() {
        try {
            final List<Objects> newChunks = chunkAllocator.getAvailableChunks();
            final JobConf jobConf = HdfsUtils.createJob(conf, ds3Client.getConnectionDetails(), bucketName, this.jobId, BulkPut.class);

            jobConf.setJarByClass(BulkPut.class);

            final File tempFile = HdfsUtils.writeToTemp(newChunks);
            final String fileListName = PathUtils.join(options.getHadoopTmpDir(), tempFile.getName());
            final Path fileListPath = hdfs.makeQualified(new Path(fileListName));

            hdfs.copyFromLocalFile(new Path(tempFile.toString()), fileListPath);
            jobConf.set(HadoopConstants.HADOOP_TMP_DIR, options.getHadoopTmpDir());

            System.out.println("Tmp File: " + fileListName);

            FileInputFormat.setInputPaths(jobConf, fileListPath);
            FileOutputFormat.setOutputPath(jobConf, hdfs.makeQualified(new Path(options.getJobOutputDir())));

            System.out.println("----- Starting put job -----");

            return jobClient.submitJob(jobConf);
        }
        catch (final Exception e) {
            this.exception = e;
            return null;
        }
    }
}
