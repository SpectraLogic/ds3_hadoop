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

import com.spectralogic.ds3.hadoop.options.ReadOptions;
import com.spectralogic.ds3.hadoop.options.WriteOptions;
import com.spectralogic.ds3client.models.bulk.Ds3Object;
import com.spectralogic.ds3client.serializer.XmlProcessingException;

import java.io.IOException;
import java.security.SignatureException;
import java.util.UUID;

class HadoopHelperStub extends HadoopHelper {
    @Override
    public Job startWriteJob(final String bucketName, final Iterable<Ds3Object> ds3Objects, final WriteOptions options) throws SignatureException, IOException, XmlProcessingException {
        System.out.println("DS3 put prime will happen here");
        return new StubJob(bucketName);
    }

    @Override
    public Job startReadJob(final String bucketName, final Iterable<Ds3Object> ds3Objects, final ReadOptions options) throws SignatureException, IOException, XmlProcessingException {
        System.out.println("DS3 get prime will happen here");
        return new StubJob(bucketName);
    }

    @Override
    public Job startReadAllJob(final String bucketName, final ReadOptions options) throws SignatureException, IOException, XmlProcessingException {
        System.out.println("DS3 get prime will happen here");
        return new StubJob(bucketName);
    }

    private class StubJob implements Job {

        private final String bucketName;

        private StubJob(final String bucketName) {
            this.bucketName = bucketName;
        }

        @Override
        public UUID getJobId() {
            return UUID.randomUUID();
        }

        @Override
        public String getBucketName() {
            return this.bucketName;
        }

        @Override
        public void transfer() throws IOException, SignatureException {
            System.out.println("Data Transfer will happen here.");
        }
    }
}
