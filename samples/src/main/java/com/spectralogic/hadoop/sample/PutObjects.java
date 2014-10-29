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

import com.spectralogic.ds3.hadoop.HadoopHelper;
import com.spectralogic.ds3.hadoop.Job;
import com.spectralogic.ds3.hadoop.options.HadoopOptions;
import com.spectralogic.ds3.hadoop.options.WriteOptions;
import com.spectralogic.ds3client.Ds3Client;
import com.spectralogic.ds3client.Ds3ClientBuilder;
import com.spectralogic.ds3client.models.Credentials;
import com.spectralogic.ds3client.models.bulk.Ds3Object;
import com.spectralogic.ds3client.serializer.XmlProcessingException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.URISyntaxException;
import java.security.SignatureException;
import java.util.ArrayList;
import java.util.List;

public class PutObjects {
    public static void main(final String[] args) throws IOException, SignatureException, XmlProcessingException, URISyntaxException {
        final Ds3Client client = Ds3ClientBuilder.create("192.168.56.102:8080", new Credentials("c3BlY3RyYQ==", "LEFsvgW2")).withHttps(false).build();

        final Configuration conf = new Configuration();

        conf.set("fs.default.name", "hdfs://192.168.56.104:9000");

        final FileSystem hdfs = FileSystem.get(conf);

        System.out.printf("Total Used Hdfs Storage: %d\n", hdfs.getStatus().getUsed());

        final HadoopOptions hadoopOptions = HadoopOptions.getDefaultOptions();
        hadoopOptions.setJobTracker(new InetSocketAddress("192.168.56.104", 50030));

        final HadoopHelper helper = HadoopHelper.wrap(client, hdfs, hadoopOptions);

        final List<Ds3Object> objects = new ArrayList<>();

        objects.add(new Ds3Object("/user/hduser/books/beowulf.txt", 301063));
        objects.add(new Ds3Object("/user/hduser/books/huckfinn.txt", 610157));
        objects.add(new Ds3Object("/user/hduser/books/taleoftwocities.txt", 792920));


        final Job job = helper.startWriteJob("books15", objects, WriteOptions.getDefault());
        job.transfer();
    }
}
