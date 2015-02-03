package com.spectralogic.ds3.hadoop;

import com.spectralogic.ds3.hadoop.mappers.BulkPut;
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
import java.security.SignatureException;
import java.util.List;
import java.util.UUID;

public class HadoopJobIterator {
    private final ChunkAllocator chunkAllocator;
    private final JobClient jobClient;
    private final UUID jobId;
    private final Configuration conf;
    private final Ds3Client ds3Client;
    private final FileSystem hdfs;
    private final JobOptions options;
    private final String bucketName;
    private final AbstractJobConfFactory jobConfFactory;

    HadoopJobIterator(final AbstractJobConfFactory jobConfFactory, final Ds3Client client, final Configuration conf, final FileSystem hdfs, final JobOptions options, final String bucketName, final MasterObjectList masterObjectList) throws IOException {
        this.jobConfFactory = jobConfFactory;
        this.conf = conf;
        this.hdfs = hdfs;
        this.options = options;
        this.bucketName = bucketName;
        this.ds3Client = client;
        this.chunkAllocator = new ChunkAllocator(client, masterObjectList.getObjects());
        this.jobClient = new JobClient(this.conf);
        this.jobId = masterObjectList.getJobId();
    }

    public boolean hasNext() {
        return chunkAllocator.hasMoreChunks();
    }

    public JobConf nextJobConf() throws IOException, SignatureException {
        final JobConf jobConf = jobConfFactory.newJobConf(this.conf, this.ds3Client.getConnectionDetails(), this.bucketName, this.jobId, BulkPut.class);
        final List<Objects> newChunks = chunkAllocator.getAvailableChunks();

        final File tempFile = HdfsUtils.writeToTemp(newChunks);
        final String fileListName = PathUtils.join(options.getHadoopTmpDir(), tempFile.getName());
        final Path fileListPath = hdfs.makeQualified(new Path(fileListName));

        hdfs.copyFromLocalFile(new Path(tempFile.toString()), fileListPath);

        if (options.getPrefix() != null) {
            jobConf.set(Constants.PREFIX, options.getPrefix());
        }

        FileInputFormat.setInputPaths(jobConf, fileListPath);
        FileOutputFormat.setOutputPath(jobConf, hdfs.makeQualified(new Path(options.getJobOutputDir())));

        return jobConf;
    }

    public RunningJob next() throws IOException, SignatureException {
        final JobConf jobConf = nextJobConf();

        return jobClient.submitJob(jobConf);
    }
}
