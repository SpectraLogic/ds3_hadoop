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

package com.spectralogic.ds3.hadoop.mappers;

import com.spectralogic.ds3.hadoop.Constants;
import com.spectralogic.ds3.hadoop.util.PathUtils;
import com.spectralogic.ds3client.Ds3Client;
import com.spectralogic.ds3client.Ds3ClientBuilder;
import com.spectralogic.ds3client.commands.GetObjectRequest;
import com.spectralogic.ds3client.models.Credentials;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;

import java.io.IOException;
import java.nio.channels.Channels;
import java.nio.channels.WritableByteChannel;
import java.security.SignatureException;
import java.util.UUID;

import static com.spectralogic.ds3.hadoop.mappers.MapperUtils.*;

public class BulkGet extends MapReduceBase implements Mapper<LongWritable, Text, Text, LongWritable> {

    private Ds3Client client;
    private FileSystem hadoopFs;
    private String bucketName;
    private String prefix;
    private UUID jobId;

    @Override
    public void configure(final JobConf conf) {
        final Ds3ClientBuilder builder = Ds3ClientBuilder.create(conf.get(Constants.ENDPOINT), new Credentials(conf.get(Constants.ACCESSKEY), conf.get(Constants.SECRETKEY)));
        final String proxy = conf.get(Constants.PROXY);
        if (proxy != null) {
            builder.withProxy(proxy);
        }
        this.client = builder.withHttps(Boolean.valueOf(conf.get(Constants.HTTPS)))
                .withCertificateVerification(Boolean.valueOf(conf.get(Constants.CERTIFICATE_VERIFICATION))).build();
        try {
            hadoopFs = FileSystem.get(new Configuration());
        } catch (final IOException e) {
            e.printStackTrace();
            hadoopFs = null;
        }
        bucketName = conf.get(Constants.BUCKET);
        prefix = conf.get(Constants.PREFIX);
        jobId = UUID.fromString(conf.get(Constants.JOB_ID));

        printJobConf(conf);
    }

    @Override
    public void map(final LongWritable longWritable, final Text value, final OutputCollector<Text, LongWritable> textLongWritableOutputCollector, final Reporter reporter) throws IOException {
        if(hadoopFs == null) {
            throw new IOException("Could not connect to the hadoop fs.");
        }

        final String rootPathName = PathUtils.join(PathUtils.getWorkingDirPath(hadoopFs), prefix);
        final FileEntry entry = FileEntry.fromString(value.toString());
        final String fileName = PathUtils.join(rootPathName, PathUtils.objPath(entry));

        final Path ds3FilePath = new Path(PathUtils.ensureStartsWithSlash(fileName));

        System.out.println("Processing file: " + entry.getFileName());
        System.out.println("Writing to file: " + fileName);

        final FSDataOutputStream fsOutputStream = hadoopFs.create(ds3FilePath);

        final WritableByteChannel outChannel = Channels.newChannel(fsOutputStream);
        try {
            client.getObject(new GetObjectRequest(bucketName, entry.getFileName(), entry.getOffset(), jobId, outChannel));
        } catch (final SignatureException e) {
            System.out.println("Failed to compute DS3 signature");
            e.printStackTrace();
            throw new IOException(e);
        }
    }
}
