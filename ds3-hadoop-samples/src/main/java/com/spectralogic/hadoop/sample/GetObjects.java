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

package com.spectralogic.hadoop.sample;

import com.spectralogic.ds3.hadoop.Ds3HadoopHelper;
import com.spectralogic.ds3.hadoop.HadoopConstants;
import com.spectralogic.ds3.hadoop.Job;
import com.spectralogic.ds3.hadoop.JobOptions;
import com.spectralogic.ds3client.Ds3Client;
import com.spectralogic.ds3client.Ds3ClientBuilder;
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
        // Setup basic logging which will log all output to the console
        BasicConfigurator.configure();
        final RootLogger logger = (RootLogger) Logger.getRootLogger();
        logger.setLevel(Level.DEBUG);

        // Create a Ds3Client specifying the endpoint of the DS3 appliance and the credentials to use
        final Ds3Client client = Ds3ClientBuilder.fromEnv().withHttps(false).build();

        // This populates the DS3 Appliance with canned test data
        final String bucketName = "readBooks18";
        FileUtils.poplulateDs3(client, bucketName);

        // Creates a Hadoop Configuration Object.  It's important that all the fields this configures are set before it is used
        final Configuration conf = Ds3HadoopHelper.createDefaultConfiguration("hdfs://172.17.0.4:9000", "172.17.0.4:8033");
        conf.set(HadoopConstants.HADOOP_JOB_UGI, "root");

        final UserGroupInformation usgi = UserGroupInformation.createRemoteUser("root");
        usgi.doAs(new PrivilegedExceptionAction<Object>() {
            @Override
            public Object run() throws Exception {

                try (final FileSystem hdfs = FileSystem.get(conf)) {
                    
                    FileUtils.cleanUpDirectory(hdfs, new Path("result"));
                    
                    // Create an instance of the Ds3HadoopHelper
                    final Ds3HadoopHelper helper = Ds3HadoopHelper.wrap(client, hdfs, conf);
                    
                    // This job is going to read all the objects out of the bucket passed in by `bucketName` to 
                    // hdfs as files
                    final Job job = helper.startReadAllJob(bucketName, JobOptions.getDefault("/tmp"));
                    
                    // This must be called for the transfer to begin
                    job.transfer();

                    System.out.println("Finished data transfer");
                    return null;
                }
            }
        });
    }
}
