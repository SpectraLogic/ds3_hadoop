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

import com.spectralogic.ds3.hadoop.mappers.BulkGet;
import com.spectralogic.ds3.hadoop.options.ReadOptions;
import com.spectralogic.ds3.hadoop.util.HdfsUtils;
import com.spectralogic.ds3.hadoop.util.PathUtils;
import com.spectralogic.ds3client.Ds3Client;
import com.spectralogic.ds3client.commands.GetAvailableJobChunksRequest;
import com.spectralogic.ds3client.commands.GetAvailableJobChunksResponse;
import com.spectralogic.ds3client.models.bulk.BulkObject;
import com.spectralogic.ds3client.models.bulk.MasterObjectList;
import com.spectralogic.ds3client.models.bulk.Objects;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.*;

import java.io.File;
import java.io.IOException;
import java.security.SignatureException;
import java.util.*;

class ReadJobImpl implements Job {

    private final Ds3Client client;
    private final UUID jobId;
    private final String bucketName;
    private final MasterObjectList masterObjectList;
    private final FileSystem hdfs;
    private final ReadOptions readOptions;
    private final Configuration conf;

    ReadJobImpl(final Ds3Client client, final FileSystem hdfs, final MasterObjectList result, final Configuration configuration, final ReadOptions readOptions) {
        this.client = client;
        this.hdfs = hdfs;
        this.masterObjectList = result;
        this.jobId = masterObjectList.getJobId();
        this.bucketName = masterObjectList.getBucketName();
        this.readOptions = readOptions;
        this.conf = configuration;
    }

    @Override
    public AbstractJobConfFactory getJobConfFactory() {
        return null;
    }

    @Override
    public void setJobConfFactory(AbstractJobConfFactory jobConfFactory) {

    }

    @Override
    public UUID getJobId() {
        return this.jobId;
    }

    @Override
    public String getBucketName() {
        return bucketName;
    }

    @Override
    public HadoopJobIterator iterator() throws IOException {
        throw new UnsupportedOperationException("For the moment please use the transfer method.");
    }

    @Override
    public void transfer() throws IOException, SignatureException {
        final ChunkGenerator chunkGenerator = new ChunkGenerator(client, jobId, masterObjectList.getObjects());
        final ObjectPartTracker partTracker = new ObjectPartTracker();

        final JobClient jobClient = new JobClient(conf);

        while(chunkGenerator.hasNext()) {
            final List<Objects> chunks = chunkGenerator.getAvailableChunks();
            partTracker.addObjects(chunks);
            final JobConf jobConf = HdfsUtils.createJob(conf, client.getConnectionDetails(), bucketName, this.jobId, BulkGet.class);

            final File tempFile = HdfsUtils.writeToTemp(chunks);
            final String fileListName = PathUtils.join(readOptions.getHadoopTmpDir(), tempFile.getName());
            final Path fileListPath = hdfs.makeQualified(new Path(fileListName));
            hdfs.copyFromLocalFile(new Path(tempFile.toString()), fileListPath);
            jobConf.set(HadoopConstants.HADOOP_TMP_DIR, readOptions.getHadoopTmpDir());

            FileInputFormat.setInputPaths(jobConf, fileListPath);
            FileOutputFormat.setOutputPath(jobConf, hdfs.makeQualified(new Path(readOptions.getJobOutputDir())));

            System.out.println("----- Starting get job -----");

            final RunningJob runningJob = jobClient.submitJob(jobConf);
            runningJob.waitForCompletion();

            System.out.println("----- Job finished running -----");
        }

        partTracker.joinParts(hdfs);
    }

    private class ChunkGenerator {

        private Ds3Client client;
        private final List<Objects> objectChunks;
        private Set<UUID> toGetSet;
        private UUID jobId;

        public ChunkGenerator(final Ds3Client client, final UUID jobId, final List<Objects> objectChunks) {
            this.client = client;
            this.jobId = jobId;
            this.objectChunks = objectChunks;
            this.toGetSet = new HashSet<>();
            populateToGetSet();
        }

        private void populateToGetSet() {
            for (final Objects objs : objectChunks) {
                this.toGetSet.add(objs.getChunkId());
            }
        }

        public boolean hasNext() {
            return !toGetSet.isEmpty();
        }

        public List<Objects> getAvailableChunks() throws IOException, SignatureException {
            while(true) {
                final GetAvailableJobChunksResponse response = client.getAvailableJobChunks(new GetAvailableJobChunksRequest(jobId));

                switch (response.getStatus()) {
                    case AVAILABLE: {
                        final List<Objects> objsList = response.getMasterObjectList().getObjects();
                        for (final Objects objs: objsList) {
                            toGetSet.remove(objs.getChunkId());
                        }
                        return objsList;
                    }
                    case RETRYLATER: {
                        try {
                            Thread.sleep(response.getRetryAfterSeconds()*1000);
                        } catch (final InterruptedException e) {
                            e.printStackTrace();
                        }
                        break;
                    }
                }
            }
        }
    }

    private class ObjectPartTracker {
        private final Map<String, SortedSet<String>> objectParts;

        public ObjectPartTracker() {
            this.objectParts = new HashMap<>();
        }

        public void addObjects(final List<Objects> newChunks) {
            for (final Objects chunk : newChunks) {
                for (final BulkObject obj : chunk.getObjects()) {
                    final Set<String> seenParts = getParts(obj);
                    if (obj.getOffset() != 0) {
                        seenParts.add(PathUtils.objPath(obj));
                    }
                }
            }
        }

        private SortedSet<String> getParts(final BulkObject obj) {
            SortedSet<String> chunks = objectParts.get(obj.getName());
            if (chunks == null) {
                chunks = new TreeSet<>();
                objectParts.put(obj.getName(), chunks);
            }
            return chunks;
        }

        public void joinParts(final FileSystem hdfs) throws IOException {
            for(final Map.Entry<String, SortedSet<String>> parts : objectParts.entrySet()) {
                if (parts.getValue().isEmpty()) {
                    continue;
                }
                final Path[] partPaths = toPaths(parts.getValue());
                hdfs.concat(new Path(parts.getKey()), partPaths);
            }
        }

        private Path[] toPaths(final Set<String> value) {
            final Path[] paths = new Path[value.size()];
            final Iterator<String> iter = value.iterator();
            int i = 0;
            while(iter.hasNext()){
                paths[i] = new Path(iter.next());
                i++;
            }

            return paths;
        }
    }
}
