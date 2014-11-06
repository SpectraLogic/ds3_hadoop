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

package com.spectralogic.ds3.hadoop.mappers;


import com.spectralogic.ds3.hadoop.Constants;
import com.spectralogic.ds3.hadoop.util.PathUtils;
import com.spectralogic.ds3.hadoop.util.SeekableReadHadoopChannel;
import com.spectralogic.ds3client.Ds3Client;
import com.spectralogic.ds3client.Ds3ClientBuilder;
import com.spectralogic.ds3client.commands.PutObjectRequest;
import com.spectralogic.ds3client.models.Credentials;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;

import java.io.IOException;
import java.security.SignatureException;
import java.util.UUID;

public class BulkPut extends MapReduceBase implements Mapper<LongWritable, Text, Text, LongWritable> {

    private Ds3Client client;
    private FileSystem hadoopFs;
    private String bucketName;
    private UUID jobId;

    @Override
    public void configure(final JobConf conf) {
        final Ds3ClientBuilder builder = Ds3ClientBuilder.create(conf.get(Constants.ENDPOINT), new Credentials(conf.get(Constants.ACCESSKEY), conf.get(Constants.SECRETKEY)));
        this.client = builder.withHttps(Boolean.valueOf(conf.get(Constants.HTTPS)))
                .withCertificateVerification(Boolean.valueOf(conf.get(Constants.CERTIFICATE_VERIFICATION))).build();
        try {
            this.hadoopFs = FileSystem.get(new Configuration());
        } catch (final IOException e) {
            e.printStackTrace();
            this.hadoopFs = null;
        }
        this.bucketName = conf.get(Constants.BUCKET);
        this.jobId = UUID.fromString(conf.get(Constants.JOB_ID));
    }

    @Override
    public void map(final LongWritable longWritable, final Text value, final OutputCollector<Text, LongWritable> output, final Reporter reporter) throws IOException {
        if (hadoopFs == null) {
            throw new IOException("Could not connect to the hadoop fs.");
        }

        final FileEntry entry = FileEntry.fromString(value.toString());

        final String rootPathName = PathUtils.getWorkingDirPath(hadoopFs);
        final String finalPath = PathUtils.join(rootPathName, entry.getFileName());
        final Path filePath = new Path(finalPath);
        System.out.println("Processing file: " + finalPath);

        try (final FSDataInputStream stream = hadoopFs.open(filePath)) {
            client.putObject(new PutObjectRequest(bucketName, entry.getFileName(), jobId, entry.getLength(), entry.getOffset(),
                    SeekableReadHadoopChannel.wrap(stream, entry.getLength(), entry.getOffset())));
        } catch (final SignatureException e) {
            System.out.println("Failed to compute DS3 signature");
            throw new IOException(e);
        }
    }
}
