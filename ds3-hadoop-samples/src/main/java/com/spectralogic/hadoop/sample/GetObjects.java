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

import com.spectralogic.ds3.hadoop.Ds3HadoopHelper;
import com.spectralogic.ds3.hadoop.HadoopConstants;
import com.spectralogic.ds3.hadoop.Job;
import com.spectralogic.ds3.hadoop.options.ReadOptions;
import com.spectralogic.ds3client.Ds3Client;
import com.spectralogic.ds3client.Ds3ClientBuilder;
import com.spectralogic.ds3client.helpers.Ds3ClientHelpers;
import com.spectralogic.ds3client.models.Credentials;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.log4j.BasicConfigurator;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.log4j.spi.RootLogger;

import java.security.PrivilegedExceptionAction;

public class GetObjects {

    public static void main(final String[] args) throws Exception {
        BasicConfigurator.configure();
        final RootLogger logger = (RootLogger) Logger.getRootLogger();
        logger.setLevel(Level.DEBUG);

        final Ds3Client client = Ds3ClientBuilder.create("192.168.56.103:8080", new Credentials("c3BlY3RyYQ==", "LEFsvgW2")).withHttps(false).build();

        final String bucketName = "readBooks18";
        FileUtils.poplulateDs3(client, bucketName);

        final Configuration conf = Ds3HadoopHelper.createDefaultConfiguration("hdfs://172.17.0.2:9000", "172.17.0.2:8033");
        conf.set(HadoopConstants.HADOOP_JOB_UGI, "root");

        final UserGroupInformation usgi = UserGroupInformation.createRemoteUser("root");
        usgi.doAs(new PrivilegedExceptionAction<Object>() {
            @Override
            public Object run() throws Exception {

                try (final FileSystem hdfs = FileSystem.get(conf)) {

                    FileUtils.cleanUpDirectory(hdfs, new Path("result"));

                    final Ds3ClientHelpers ds3Helper = Ds3ClientHelpers.wrap(client);
                    ds3Helper.ensureBucketExists(bucketName);

                    final Ds3HadoopHelper helper = Ds3HadoopHelper.wrap(client, hdfs, conf);
                    final Job job = helper.startReadAllJob(bucketName, ReadOptions.getDefault());
                    job.transfer();

                    System.out.println("Finished data transfer");
                    return null;
                }
            }
        });
    }
}
