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

import com.spectralogic.ds3client.Ds3Client;
import com.spectralogic.ds3client.models.bulk.MasterObjectList;
import org.apache.hadoop.fs.FileSystem;

import java.io.IOException;
import java.util.UUID;

class ReadJobImpl implements Job {

    private final Ds3Client client;
    private final UUID jobId;
    private final String bucketName;
    private final MasterObjectList masterObjectList;
    private final FileSystem hdfs;

    ReadJobImpl(final Ds3Client client, final FileSystem hdfs, final MasterObjectList result) {
        this.client = client;
        this.hdfs = hdfs;
        this.masterObjectList = result;
        this.jobId = masterObjectList.getJobId();
        this.bucketName = masterObjectList.getBucketName();
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
    public void transfer() throws IOException {

        // Loop through each chunk and submit a new job


        /*
        final File tempFile = HdfsUtils.writeToTemp(result);

        final String fileListFile = PathUtils.join(conf.get(Constants.HADOOP_TMP_DIR), tempFile.getName());

        this.hdfs.copyFromLocalFile(new Path(tempFile.toString()), new Path(conf.get(Constants.HADOOP_TMP_DIR)));

        FileInputFormat.setInputPaths(conf, fileListFile);
        FileOutputFormat.setOutputPath(conf, getOutputDirectory());

        final RunningJob runningJob = JobClient.runJob(conf);
        runningJob.waitForCompletion();
        */
    }

}
