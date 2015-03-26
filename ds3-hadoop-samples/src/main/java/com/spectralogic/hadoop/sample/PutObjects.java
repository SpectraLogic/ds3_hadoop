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
import com.spectralogic.ds3.hadoop.JobOptions;
import com.spectralogic.ds3client.Ds3Client;
import com.spectralogic.ds3client.Ds3ClientBuilder;
import com.spectralogic.ds3client.models.Credentials;
import com.spectralogic.ds3client.models.bulk.Ds3Object;
import com.spectralogic.ds3client.serializer.XmlProcessingException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.security.UserGroupInformation;

import java.io.IOException;
import java.net.URISyntaxException;
import java.security.PrivilegedExceptionAction;
import java.security.SignatureException;
import java.util.List;

import org.apache.log4j.Logger;
import org.apache.log4j.spi.RootLogger;
import org.apache.log4j.Level;
import org.apache.log4j.BasicConfigurator;

public class PutObjects {

    public static void main(final String[] args) throws IOException, SignatureException, XmlProcessingException, URISyntaxException, InterruptedException {
        // Setup basic logging which will log all output to the console
        BasicConfigurator.configure();
        final RootLogger logger = (RootLogger) Logger.getRootLogger();
        logger.setLevel(Level.DEBUG);
        
        // Create a Ds3Client specifying the endpoint of the DS3 appliance and the credentials to use
        final Ds3Client client = Ds3ClientBuilder.create("10.1.18.60", new Credentials("c3BlY3RyYQ==", "qQiFXeVZ")).withHttps(false).build();
        
        // Creates a Hadoop Configuration Object.  It's important that all the fields this configures are set before it is used
        final Configuration conf = Ds3HadoopHelper.createDefaultConfiguration("hdfs://172.17.0.14:9000", "172.17.0.14:8033");
        conf.set(HadoopConstants.HADOOP_JOB_UGI, "root");

        final UserGroupInformation usgi = UserGroupInformation.createRemoteUser("root");
        usgi.doAs(new PrivilegedExceptionAction<Object>() {
            @Override
            public Object run() throws Exception {
                try (final FileSystem hdfs = FileSystem.get(conf)) {
                    //FileUtils.cleanUpDirectory(hdfs, new Path("result"));
                    
                    // Convience method to populate Hadoop with canned test data.  This returns the list of test
                    // files that were uploaded to the cluster which we will then transfer via DS3
                    final List<Ds3Object> objects = FileUtils.populateHadoop(hdfs);
                    
                    // Create an instance of the Ds3HadoopHelper
                    final Ds3HadoopHelper helper = Ds3HadoopHelper.wrap(client, hdfs, conf);

                    // Create a job configuration object to tell the helper where the hadoop temp dir is
                    // as well as a prefix to use when getting the objects from HDFS.  This allows you
                    // to use absolute paths.
                    final JobOptions options = JobOptions.getDefault("/tmp");
                    options.setPrefix("/user/root");

                    // This creates the DS3 transfer job to buckets 'books'
                    final Job job = helper.startWriteJob("books", objects, options);
                    
                    // This must be called for the transfer to begin
                    job.transfer();
                }
                return null;
            }
        });
    }
}
