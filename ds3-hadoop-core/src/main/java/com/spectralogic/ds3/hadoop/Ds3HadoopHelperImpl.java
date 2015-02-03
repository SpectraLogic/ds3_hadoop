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

import com.google.common.collect.Lists;
import com.spectralogic.ds3.hadoop.util.ListUtils;
import com.spectralogic.ds3client.Ds3Client;
import com.spectralogic.ds3client.commands.BulkGetRequest;
import com.spectralogic.ds3client.commands.BulkPutRequest;
import com.spectralogic.ds3client.commands.GetBucketRequest;
import com.spectralogic.ds3client.helpers.Ds3ClientHelpers;
import com.spectralogic.ds3client.models.Contents;
import com.spectralogic.ds3client.models.ListBucketResult;
import com.spectralogic.ds3client.models.bulk.ChunkClientProcessingOrderGuarantee;
import com.spectralogic.ds3client.models.bulk.Ds3Object;
import com.spectralogic.ds3client.models.bulk.MasterObjectList;
import com.spectralogic.ds3client.serializer.XmlProcessingException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;

import java.io.IOException;
import java.security.SignatureException;
import java.util.ArrayList;
import java.util.List;

class Ds3HadoopHelperImpl extends Ds3HadoopHelper {

    private final Ds3Client client;
    private final Ds3ClientHelpers helpers;
    private final FileSystem hdfs;
    private final Configuration conf;


    public Ds3HadoopHelperImpl(final Ds3Client client, final FileSystem hdfs, final Configuration configuration) {
        this.client = client;
        this.hdfs = hdfs;
        this.helpers = Ds3ClientHelpers.wrap(client);
        this.conf = configuration;
    }

    @Override
    public Job startWriteJob(final String bucketName, final Iterable<Ds3Object> ds3Objects, final JobOptions options) throws SignatureException, IOException, XmlProcessingException {

        helpers.ensureBucketExists(bucketName);

        final MasterObjectList result = this.client.bulkPut(new BulkPutRequest(bucketName, Lists.newArrayList(ds3Objects))).getResult();

        return new WriteJobImpl(client, hdfs, result, conf, options);
    }

    @Override
    public Job startReadJob(final String bucketName, final Iterable<Ds3Object> ds3Objects, final JobOptions options)throws SignatureException, IOException, XmlProcessingException {

        final MasterObjectList result = this.client.bulkGet(new BulkGetRequest(bucketName,
                    Lists.newArrayList(ds3Objects))
                .withChunkOrdering(ChunkClientProcessingOrderGuarantee.NONE))
                .getResult();

        return new ReadJobImpl(client, hdfs, result, conf, options);
    }

    @Override
    public Job startReadAllJob(final String bucketName, final JobOptions options) throws SignatureException, IOException, XmlProcessingException {
        // ------------- Get file list from DS3 -------------
        final ListBucketResult fileList = this.client.getBucket(new GetBucketRequest(bucketName)).getResult();
        final List<Ds3Object> objects = convertToList(fileList);

        // prime ds3
        final MasterObjectList result = this.client.bulkGet(new BulkGetRequest(bucketName,
                    Lists.newArrayList(ListUtils.filterDirectories(objects)))
                .withChunkOrdering(ChunkClientProcessingOrderGuarantee.NONE)).
                getResult();

        return new ReadJobImpl(client, hdfs, result, conf, options);
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
