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

package com.spectralogic.hadoop.sample;

import com.spectralogic.ds3.hadoop.HadoopConstants;
import com.spectralogic.ds3.hadoop.HadoopHelper;
import com.spectralogic.ds3.hadoop.Job;
import com.spectralogic.ds3.hadoop.options.HadoopOptions;
import com.spectralogic.ds3.hadoop.options.WriteOptions;
import com.spectralogic.ds3client.Ds3Client;
import com.spectralogic.ds3client.Ds3ClientBuilder;
import com.spectralogic.ds3client.commands.PutObjectRequest;
import com.spectralogic.ds3client.models.Credentials;
import com.spectralogic.ds3client.models.bulk.Ds3Object;
import com.spectralogic.ds3client.serializer.XmlProcessingException;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.security.UserGroupInformation;

import java.io.FilterInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.InetSocketAddress;
import java.net.URISyntaxException;
import java.security.PrivilegedAction;
import java.security.PrivilegedExceptionAction;
import java.security.SignatureException;
import java.util.ArrayList;
import java.util.List;

public class PutObjects {

    public static void main(final String[] args) throws IOException, SignatureException, XmlProcessingException, URISyntaxException, InterruptedException {
        final Ds3Client client = Ds3ClientBuilder.create("192.168.56.103:8080", new Credentials("c3BlY3RyYQ==", "LEFsvgW2")).withHttps(false).build();

        final Configuration conf = new Configuration();
        final UserGroupInformation usgi = UserGroupInformation.createRemoteUser("root");

        usgi.doAs(new PrivilegedExceptionAction<Object>() {
            @Override
            public Object run() throws Exception {
                conf.set(HadoopConstants.FS_DEFAULT_NAME, "hdfs://172.17.0.3:9000");
                conf.set(HadoopConstants.HADOOP_JOB_UGI, "root");

                try (final FileSystem hdfs = FileSystem.get(conf)) {

                    System.out.printf("Total Used Hdfs Storage: %d\n", hdfs.getStatus().getUsed());

                    final HadoopOptions hadoopOptions = HadoopOptions.getDefaultOptions();
                    hadoopOptions.setJobTracker(new InetSocketAddress("172.17.0.3", 8033));

                    final HadoopHelper helper = HadoopHelper.wrap(client, hdfs, hadoopOptions);

                    final List<Ds3Object> objects = populateTestData(hdfs);

                    final Job job = helper.startWriteJob("books39", objects, WriteOptions.getDefault());
                    job.transfer();
                }
                return null;
            }
        });
    }

    private static List<Ds3Object> populateTestData(final FileSystem hdfs) throws IOException {

        final String[] resources = new String[]{"books/beowulf.txt", "books/sherlock_holmes.txt", "books/tale_of_two_cities.txt", "books/ulysses.txt"};
        final List<Ds3Object> objects = new ArrayList<>();

        for (final String resourceName : resources) {

            System.out.println("Processing: " + resourceName);
            final Path path = new Path("/user/root", resourceName);

            try (final CountingInputStream inputStream = new CountingInputStream(PutObjects.class.getClassLoader().getResourceAsStream(resourceName));
            final FSDataOutputStream outputStream = hdfs.create(path, true)) {

                IOUtils.copy(inputStream, outputStream);
                objects.add(new Ds3Object(path.toString(), inputStream.byteCount));
            }
        }

        return objects;
    }

    private static class CountingInputStream extends FilterInputStream {

        private long byteCount = 0;
        private final InputStream in;

        public CountingInputStream(final InputStream in) {
            super(in);

            if (in == null) {
                throw new NullPointerException("'in' cannot be null");
            }

            this.in = in;
        }

        @Override
        public int read() throws IOException {
            byteCount++;
            return in.read();
        }

        @Override
        public int read(final byte[] buf) throws IOException {
            final int bytesRead = in.read(buf);
            byteCount += bytesRead;
            return bytesRead;
        }

        @Override
        public int read(final byte[] buf, final int offset, final int length) throws IOException {
            final int bytesRead = in.read(buf, offset, length);
            byteCount += bytesRead;
            return bytesRead;
        }

        public long getByteCount() {
            return byteCount;
        }
    }
}
