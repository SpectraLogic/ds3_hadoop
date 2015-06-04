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

import com.spectralogic.ds3client.Ds3Client;
import com.spectralogic.ds3client.models.bulk.Ds3Object;
import com.spectralogic.ds3client.serializer.XmlProcessingException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;

import java.io.IOException;
import java.security.SignatureException;
import java.util.UUID;

/**
 * Base class that defines Hadoop Utilities to transfer objects between DS3 and a Hadoop Cluster.
 */
public abstract class Ds3HadoopHelper {

    /**
     * Wrap a Ds3Client with the Ds3HadoopHelper which allows for sending requests to a Hadoop Cluster to interact with DS3
     * @param client The Ds3Client to wrap
     * @param hdfs The FileSystem object for the remote Hadoop Cluster, typically backed by HDFS
     * @param configuration The base Hadoop Cluster Configuration
     */
    public static Ds3HadoopHelper wrap(final Ds3Client client, final FileSystem hdfs, final Configuration configuration) {
        return new Ds3HadoopHelperImpl(client, hdfs, configuration);
    }

    /**
     * Initiates a Ds3 Bulk Put with files that are stored in the Hadoop FileSystem.  **Note:** this call does not send the objects
     * @param bucketName The Ds3 bucket to send the objects to
     * @param ds3Objects The list of objects to send that are located in the cluster
     * @param options Options to control how the MapReduce job should behave
     */
    public abstract Job startWriteJob(final String bucketName, final Iterable<Ds3Object> ds3Objects, final JobOptions options) throws SignatureException, IOException, XmlProcessingException;

    public abstract Job startReadJob(final String bucketName, final Iterable<Ds3Object> ds3Objects, final JobOptions options) throws SignatureException, IOException, XmlProcessingException;

    public abstract Job startReadAllJob(final String bucketName, final JobOptions options) throws SignatureException, IOException, XmlProcessingException;

    /**
     * Restarts transmitting the files for a write job.  Any files that are not "IN_CACHE" and who's chunks have
     * not been completely written to cache will be launched in a new Hadoop Job to be transferred.
     */
    public abstract Job recoverWriteJob(final UUID jobId, final JobOptions options) throws SignatureException, IOException, XmlProcessingException, InvalidJobStatusException;
    /**
     * This returns a Hadoop Configuration object with the 'fs.default.name' set to {@param nameNode}, 'mapred.job.tracker' set to {@param jobTracker}, and 'mapreduce.framework.name' set to 'yarn'.
     * @param nameNode The url for the name node
     * @param jobTracker The url for the job tracker where map reduce jobs will be launched
     */
    public static Configuration createDefaultConfiguration(final String nameNode, final String jobTracker) {
        final Configuration conf = new Configuration();
        conf.set(HadoopConstants.MAPREDUCE_FRAMEWORK_NAME, HadoopConstants.YARN);
        conf.set(HadoopConstants.FS_DEFAULT_NAME, nameNode);
        conf.set(HadoopConstants.MAPRED_JOB_TRACKER, jobTracker);

        return conf;
    }
}
