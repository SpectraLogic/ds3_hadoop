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
import com.spectralogic.ds3.hadoop.options.HadoopOptions;
import com.spectralogic.ds3.hadoop.options.ReadOptions;
import com.spectralogic.ds3client.Ds3Client;
import com.spectralogic.ds3client.Ds3ClientBuilder;
import com.spectralogic.ds3client.models.Credentials;
import com.spectralogic.ds3client.models.bulk.Ds3Object;
import com.spectralogic.ds3client.serializer.XmlProcessingException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.log4j.BasicConfigurator;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.log4j.spi.RootLogger;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.security.PrivilegedExceptionAction;
import java.security.SignatureException;
import java.util.List;

public class GetObjects {

    public static void main(final String[] args) throws IOException, InterruptedException {
        BasicConfigurator.configure();

        final RootLogger logger = (RootLogger) Logger.getRootLogger();
        logger.setLevel(Level.DEBUG);

        final Ds3Client client = Ds3ClientBuilder.create("192.168.56.103:8080", new Credentials("c3BlY3RyYQ==", "LEFsvgW2")).withHttps(false).build();

        final String bucketName = "readBooks01";

        final Configuration conf = new Configuration();
        final UserGroupInformation usgi = UserGroupInformation.createRemoteUser("root");

        usgi.doAs(new PrivilegedExceptionAction<Object>() {
            @Override
            public Object run() throws Exception {
                conf.set(HadoopConstants.FS_DEFAULT_NAME, "hdfs://172.17.0.3:9000");
                conf.set(HadoopConstants.HADOOP_JOB_UGI, "root");

                try (final FileSystem hdfs = FileSystem.get(conf)) {

                    final Path resultDir = new Path("result");
                    if (hdfs.exists(resultDir)) {
                        System.out.println("Cleaning up the result directory.");
                        hdfs.delete(resultDir, true);
                    }

                    System.out.printf("Total Used Hdfs Storage: %d\n", hdfs.getStatus().getUsed());

                    final HadoopOptions hadoopOptions = HadoopOptions.getDefaultOptions();
                    hadoopOptions.setJobTracker(new InetSocketAddress("172.17.0.3", 8033));

                    final List<Ds3Object> objects = FileUtils.populateHadoop(hdfs);
                    FileUtils.poplulateDs3(client, bucketName);

                    transferFromBlackPearl(client, hdfs, hadoopOptions, bucketName);
                    return null;
                }
            }
        });
    }

    public static void transferFromBlackPearl(final Ds3Client client, final FileSystem hdfs, final HadoopOptions hadoopOptions, final String bucketName) throws XmlProcessingException, SignatureException, IOException {
        final HadoopHelper helper = HadoopHelper.wrap(client, hdfs, hadoopOptions);
        helper.startReadAllJob(bucketName, ReadOptions.getDefault());
    }
}
