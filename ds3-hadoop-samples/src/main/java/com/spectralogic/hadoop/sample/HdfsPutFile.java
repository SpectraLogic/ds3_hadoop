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
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.security.UserGroupInformation;

import java.io.IOException;
import java.io.InputStream;
import java.security.PrivilegedExceptionAction;

public class HdfsPutFile {
    public static void main(final String[] args) throws IOException, InterruptedException {

        final Configuration conf = new Configuration();
        final UserGroupInformation usgi = UserGroupInformation.createRemoteUser("root");

        usgi.doAs(new PrivilegedExceptionAction<Object>() {
            @Override
            public Object run() throws Exception {
                conf.set(HadoopConstants.FS_DEFAULT_NAME, "hdfs://192.168.56.102:9000");
                conf.set(HadoopConstants.HADOOP_JOB_UGI, "root");

                try (final FileSystem fs = FileSystem.get(conf)) {

                    System.out.printf("Total Used Hdfs Storage: %d\n", fs.getStatus().getUsed());

                    final DistributedFileSystem hdfs = (DistributedFileSystem) fs ;
                    final DatanodeInfo[] dataNodeStats = hdfs.getDataNodeStats();

                    System.out.println("Data Nodes:");
                    for (final DatanodeInfo info : dataNodeStats) {
                        System.out.println(info.toString());
                    }

                    final String resourceName = "books/beowulf.txt";

                    final Path path = new Path("/user/root", resourceName);

                    try (final InputStream inputStream = HdfsPutFile.class.getClassLoader().getResourceAsStream(resourceName);
                         final FSDataOutputStream outputStream = fs.create(path, true)) {

                        IOUtils.copy(inputStream, outputStream);
                    }
                }
                return null;
            }
        });
    }
}
