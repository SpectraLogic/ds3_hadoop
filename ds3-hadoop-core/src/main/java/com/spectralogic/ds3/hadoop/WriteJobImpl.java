/*
 * ******************************************************************************
 *   Copyright 2014 Spectra Logic Corporation. All Rights Reserved.
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


import com.spectralogic.ds3.hadoop.mappers.BulkPut;
import com.spectralogic.ds3.hadoop.options.HadoopOptions;
import com.spectralogic.ds3.hadoop.options.WriteOptions;
import com.spectralogic.ds3.hadoop.util.HdfsUtils;
import com.spectralogic.ds3.hadoop.util.PathUtils;
import com.spectralogic.ds3client.Ds3Client;
import com.spectralogic.ds3client.commands.AllocateJobChunkRequest;
import com.spectralogic.ds3client.commands.AllocateJobChunkResponse;
import com.spectralogic.ds3client.models.bulk.MasterObjectList;
import com.spectralogic.ds3client.models.bulk.Objects;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.filecache.DistributedCache;
import org.apache.hadoop.mapred.*;

import java.io.File;
import java.io.IOException;
import java.security.SignatureException;
import java.util.*;

class WriteJobImpl implements Job {
    private final Ds3Client ds3Client;
    private final FileSystem hdfs;
    private final MasterObjectList masterObjectList;
    private final UUID jobId;
    private final String bucketName;
    private final WriteOptions options;
    private final HadoopOptions hadoopOptions;

    public WriteJobImpl(final Ds3Client ds3Client1, final FileSystem hdfs, final MasterObjectList masterObjectList, final HadoopOptions hadoopOptions, final WriteOptions options) {
        this.ds3Client = ds3Client1;
        this.hdfs = hdfs;
        this.masterObjectList = masterObjectList;
        this.jobId = masterObjectList.getJobId();
        this.bucketName = masterObjectList.getBucketName();
        this.hadoopOptions = hadoopOptions;
        this.options = options;
    }

    @Override
    public UUID getJobId() {
        return this.jobId;
    }

    @Override
    public String getBucketName() {
        return this.bucketName;
    }

    @Override
    public void transfer() throws IOException, SignatureException {
        final ChunkAllocator chunkAllocator = new ChunkAllocator(this.masterObjectList.getObjects());
        final Configuration conf = this.hdfs.getConf();
        final Configuration clientConf = new Configuration(conf);
        clientConf.addResource(this.hadoopOptions.getConfig()); 
        final JobClient jobClient = new JobClient(this.hadoopOptions.getJobTracker(), clientConf); 

        //final Path fatJar = HdfsUtils.setupJobJarFile(this.hdfs, this.options.getHadoopTmpDir(), BulkPut.class); 

        while (chunkAllocator.hasMoreChunks()) {
            final List<Objects> newChunks = chunkAllocator.getAvailableChunks();
            final JobConf jobConf = HdfsUtils.createJob(ds3Client.getConnectionDetails(), bucketName, this.jobId, BulkPut.class);

            System.out.println("Setting jobTracker to: " + hadoopOptions.getJobTracker().toString());

            jobConf.set("mapreduce.jobtracker.address", hadoopOptions.getJobTracker().toString());

            //System.out.println("Remote Path: " + fatJar.toString());
            //DistributedCache.addFileToClassPath(fatJar, jobConf, hdfs);
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

            final RunningJob runningJob = jobClient.submitJob(jobConf);
            runningJob.waitForCompletion();

            System.out.println("Error result: " + runningJob.getFailureInfo());

            System.out.println("----- Job finished running -----");
        }
    }

    private class ChunkAllocator {

        final PriorityQueue<Objects> chunks;

        public ChunkAllocator(final List<Objects> objs) {
            this.chunks = new PriorityQueue<>(objs.size(), new Comparator<Objects>() {
                @Override
                public int compare(final Objects o1, final Objects o2) {
                    return Long.compare(o1.getChunkNumber(), o2.getChunkNumber());
                }
            });
            this.chunks.addAll(objs);
        }

        public boolean hasMoreChunks() {
            return !chunks.isEmpty();
        }

        private AllocateJobChunkResponse allocateChunk(final UUID id) throws IOException, SignatureException {
            return ds3Client.allocateJobChunk(new AllocateJobChunkRequest(id));
        }

        public List<Objects> getAvailableChunks() throws IOException, SignatureException {
            final List<Objects> newChunks = new ArrayList<>();

            boolean continueAllocatingChunks = true;
            while(continueAllocatingChunks && hasMoreChunks()){
                final AllocateJobChunkResponse response = allocateChunk(chunks.poll().getChunkId());
                final AllocateJobChunkResponse.Status status = response.getStatus();

                if (status == AllocateJobChunkResponse.Status.RETRYLATER && newChunks.isEmpty()) {
                    try {
                        Thread.sleep(response.getRetryAfterSeconds()*1000);
                    } catch (final InterruptedException e) {
                        e.printStackTrace();
                    }
                }
                else if (status == AllocateJobChunkResponse.Status.RETRYLATER) {
                    continueAllocatingChunks = false;
                }
                else {
                    newChunks.add(response.getObjects());
                }
            }
            return newChunks;
        }
    }

}
