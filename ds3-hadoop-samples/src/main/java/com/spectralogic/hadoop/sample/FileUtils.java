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

import com.spectralogic.ds3client.Ds3Client;
import com.spectralogic.ds3client.helpers.Ds3ClientHelpers;
import com.spectralogic.ds3client.models.bulk.Ds3Object;
import com.spectralogic.ds3client.serializer.XmlProcessingException;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.*;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.channels.FileChannel;
import java.nio.channels.SeekableByteChannel;
import java.nio.file.Paths;
import java.security.SignatureException;
import java.util.ArrayList;
import java.util.List;

public class FileUtils {
    private static final String[] resources = new String[]{"books/beowulf.txt", "books/sherlock_holmes.txt", "books/tale_of_two_cities.txt", "books/ulysses.txt"};

    public static List<Ds3Object> populateHadoop(final FileSystem hdfs) throws IOException {
        final List<Ds3Object> objects = new ArrayList<>();

        for (final String resourceName : resources) {

            System.out.println("Processing: " + resourceName);
            final Path path = new Path("/user/root", resourceName);

            try (final CountingInputStream inputStream = new CountingInputStream(FileUtils.class.getClassLoader().getResourceAsStream(resourceName));
                 final FSDataOutputStream outputStream = hdfs.create(path, true)) {

                IOUtils.copy(inputStream, outputStream);
                objects.add(new Ds3Object(resourceName, inputStream.byteCount));
            }
        }

        return objects;
    }

    public static List<Ds3Object> poplulateDs3(final Ds3Client client, final String bucketName) throws IOException, SignatureException, XmlProcessingException {
        final Ds3ClientHelpers helpers = Ds3ClientHelpers.wrap(client);
	helpers.ensureBucketExists(bucketName);
        final List<Ds3Object> objects = new ArrayList<>();

        for (final String resourceName : resources) {
            System.out.println("Adding: " + resourceName);
            final File file =  new File (FileUtils.class.getClassLoader().getResource(resourceName).getFile());

            objects.add(new Ds3Object(resourceName, file.length()));
        }

        final Ds3ClientHelpers.Job writeJob = helpers.startWriteJob(bucketName, objects);

        writeJob.transfer(new Ds3ClientHelpers.ObjectChannelBuilder() {
            @Override
            public SeekableByteChannel buildChannel(final String s) throws IOException {
                System.out.println("Building the Resource Channel for: " + s);
                final URL fileName = FileUtils.class.getClassLoader().getResource(s);
                try {
                    return FileChannel.open(Paths.get(fileName.toURI()));
                } catch (final URISyntaxException e) {
                    throw new IOException(e);
                }
            }
        });

        return objects;
    }

    public static void cleanUpDirectory(final FileSystem hdfs, final Path path) throws IOException {
        if (hdfs.exists(path)) {
            System.out.println("Cleaning up the result directory.");
            hdfs.delete(path, true);
        }
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
