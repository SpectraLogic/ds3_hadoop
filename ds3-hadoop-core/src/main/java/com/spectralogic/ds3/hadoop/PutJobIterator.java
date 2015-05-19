/*
 * ******************************************************************************
 *   Copyright 2014-2015 Spectra Logic Corporation. All Rights Reserved.
 *   Licensed under the Apache License, Version 2.0 (the "License"). You may not use
 *   this file except in compliance with the License. A copy of the License is located at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 *   or in the "license" file accompanying this file.
 *   This file is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 *   CONDITIONS OF ANY KIND, either express or implied. See the License for the
 *   specific language governing permissions and limitations under the License.
 * ****************************************************************************
 */

package com.spectralogic.ds3.hadoop;

import com.spectralogic.ds3.hadoop.util.HdfsUtils;
import com.spectralogic.ds3.hadoop.util.PathUtils;
import com.spectralogic.ds3.hadoop.util.UnsafeDeleteException;
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

public class PutJobIterator implements HadoopJobIterator {
    private final ChunkAllocator chunkAllocator;
    private final JobClient jobClient;
    private final UUID jobId;
    private final Configuration conf;
    private final Ds3Client ds3Client;
    private final FileSystem hdfs;
    private final JobOptions options;
    private final String bucketName;
    private final AbstractJobConfFactory jobConfFactory;
    private final Class<? extends Mapper> mapperClass;

    PutJobIterator(final AbstractJobConfFactory jobConfFactory, final Ds3Client client, final Configuration conf, final FileSystem hdfs, final JobOptions options, final String bucketName, final MasterObjectList masterObjectList, final Class<? extends Mapper> mapperClass) throws IOException {
        this.jobConfFactory = jobConfFactory;
        this.conf = conf;
        this.hdfs = hdfs;
        this.mapperClass = mapperClass;
        this.options = options;
        this.bucketName = bucketName;
        this.ds3Client = client;
        this.chunkAllocator = new ChunkAllocator(client, masterObjectList.getJobId(), options.getNumberOfChunksPerJob(), masterObjectList.getObjects());
        this.jobClient = new JobClient(this.conf);
        this.jobId = masterObjectList.getJobId();
    }

    public boolean hasNext() {
        return chunkAllocator.hasMoreChunks();
    }

    public JobConf nextJobConf() throws IOException, SignatureException, TransferJobException {
        final JobConf jobConf = jobConfFactory.newJobConf(this.conf, this.ds3Client.getConnectionDetails(), this.options, this.bucketName, this.jobId, mapperClass);
        final List<Objects> newChunks = chunkAllocator.getAvailableChunks();

        final File tempFile = HdfsUtils.writeToTemp(newChunks);
        final String fileListName = PathUtils.join(options.getHadoopTmpDir(), tempFile.getName());
        final Path fileListPath = hdfs.makeQualified(new Path(fileListName));

        final Path outputPath = new Path(options.getJobOutputDir());

        hdfs.copyFromLocalFile(new Path(tempFile.toString()), fileListPath);

        if (options.getPrefix() != null) {
            jobConf.set(Constants.PREFIX, options.getPrefix());
        }

        try {
            HdfsUtils.safeDirectoryDelete(hdfs, outputPath);
        } catch (final UnsafeDeleteException e) {
            throw new TransferJobException(e);
        }

        FileInputFormat.setInputPaths(jobConf, fileListPath);
        FileOutputFormat.setOutputPath(jobConf, hdfs.makeQualified(outputPath));

        return jobConf;
    }

    public RunningJob next() throws IOException, SignatureException, TransferJobException {
        final JobConf jobConf = nextJobConf();

        return jobClient.submitJob(jobConf);
    }
}
