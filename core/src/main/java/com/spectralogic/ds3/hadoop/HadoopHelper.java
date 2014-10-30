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

import com.spectralogic.ds3.hadoop.options.HadoopOptions;
import com.spectralogic.ds3.hadoop.options.ReadOptions;
import com.spectralogic.ds3.hadoop.options.WriteOptions;
import com.spectralogic.ds3client.Ds3Client;
import com.spectralogic.ds3client.models.bulk.Ds3Object;
import com.spectralogic.ds3client.serializer.XmlProcessingException;
import org.apache.hadoop.fs.FileSystem;

import java.io.IOException;
import java.security.SignatureException;

/**
 * Base class that defines Hadoop Utilities to transfer objects between DS3 and a Hadoop Cluster.
 */
public abstract class HadoopHelper {

    public static HadoopHelper wrap(final Ds3Client client, final FileSystem hdfs) {
        return new HadoopHelperStub();
        //return new HadoopHelperImpl(client, hdfs);
    }

    public static HadoopHelper wrap(final Ds3Client client, final FileSystem hdfs, final HadoopOptions hadoopOptions) {
        return new HadoopHelperStub();
        //return new HadoopHelperImpl(client, hdfs, hadoopOptions);
    }

    public abstract Job startWriteJob(final String bucketName, final Iterable<Ds3Object> ds3Objects, final WriteOptions options) throws SignatureException, IOException, XmlProcessingException;

    public abstract Job startReadJob(final String bucketName, final Iterable<Ds3Object> ds3Objects, final ReadOptions options) throws SignatureException, IOException, XmlProcessingException;

    public abstract Job startReadAllJob(final String bucketName, final ReadOptions options) throws SignatureException, IOException, XmlProcessingException;
}
