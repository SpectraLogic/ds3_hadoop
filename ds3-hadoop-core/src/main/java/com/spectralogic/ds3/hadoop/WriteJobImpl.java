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


import com.spectralogic.ds3.hadoop.options.WriteOptions;
import com.spectralogic.ds3client.Ds3Client;
import com.spectralogic.ds3client.models.bulk.MasterObjectList;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.mapred.*;

import java.io.IOException;
import java.util.*;

class WriteJobImpl implements Job {
    private final Ds3Client ds3Client;
    private final FileSystem hdfs;
    private final MasterObjectList masterObjectList;
    private final UUID jobId;
    private final String bucketName;
    private final WriteOptions options;
    private final Configuration conf;

    private AbstractJobConfFactory jobConfFactory = new DefaultJobConfFactory();

    public WriteJobImpl(final Ds3Client ds3Client1, final FileSystem hdfs, final MasterObjectList masterObjectList, final Configuration configuration, final WriteOptions options) {
        this.ds3Client = ds3Client1;
        this.hdfs = hdfs;
        this.masterObjectList = masterObjectList;
        this.jobId = masterObjectList.getJobId();
        this.bucketName = masterObjectList.getBucketName();
        this.options = options;
        this.conf = configuration;
    }

    @Override
    public AbstractJobConfFactory getJobConfFactory() {
        return jobConfFactory;
    }

    @Override
    public void setJobConfFactory(final AbstractJobConfFactory jobConfFactory) {
        this.jobConfFactory = jobConfFactory;
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
    public HadoopJobIterator iterator() throws IOException {
        return new HadoopJobIterator(jobConfFactory, this.ds3Client, this.conf, this.hdfs, this.options, this.bucketName, this.masterObjectList);
    }

    @Override
    public void transfer() throws Exception {
        final HadoopJobIterator iter = this.iterator();
        while(iter.hasNext()) {
            final RunningJob job = iter.next();
            job.waitForCompletion();

            if (!job.isSuccessful()) {
                System.out.print(job.getFailureInfo());
            }
        }
    }
}
