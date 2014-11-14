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

package com.spectralogic.ds3.hadoop.util;

import com.spectralogic.ds3.hadoop.Constants;
import com.spectralogic.ds3.hadoop.Ds3HadoopHelper;
import com.spectralogic.ds3.hadoop.HadoopConstants;
import com.spectralogic.ds3.hadoop.mappers.FileEntry;
import com.spectralogic.ds3client.models.bulk.BulkObject;
import com.spectralogic.ds3client.models.bulk.Ds3Object;
import com.spectralogic.ds3client.models.bulk.Objects;
import com.spectralogic.ds3client.networking.ConnectionDetails;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;
import org.apache.hadoop.yarn.conf.YarnConfiguration;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

public class HdfsUtils {
    public static File writeToTemp(final List<Objects> masterObjectList) throws IOException {
        final File tempFile = File.createTempFile("migrator","dat");
        final PrintWriter writer = new PrintWriter(new FileWriter(tempFile));

        /* TODO: We will have to update this to take into account different nodes post 1.0. */
        for(final Objects objects: masterObjectList) {
            for(final BulkObject object: objects.getObjects()) {
                writer.printf("%s\n", FileEntry.fromBulkObject(object).toString());
            }
        }

        //flush the contents to make sure they are on disk before writing to hdfs
        writer.flush();
        writer.close();

        return tempFile;
    }

    public static JobConf createJob(final Configuration baseConfig, final ConnectionDetails connectionDetails, final String bucketName, final UUID jobId, final Class<? extends Mapper> mapperClass) {
        final JobConf conf = new JobConf(baseConfig, Ds3HadoopHelper.class);
        conf.setJobName(Constants.JOB_NAME);

        conf.set(Constants.HTTPS, String.valueOf(connectionDetails.isHttps()));
        conf.set(Constants.CERTIFICATE_VERIFICATION, String.valueOf(connectionDetails.isCertificateVerification()));
        conf.set(Constants.BUCKET, bucketName);
        conf.set(Constants.ACCESSKEY, connectionDetails.getCredentials().getClientId());
        conf.set(Constants.SECRETKEY, connectionDetails.getCredentials().getKey());
        conf.set(Constants.ENDPOINT, connectionDetails.getEndpoint());
        conf.set(Constants.JOB_ID, jobId.toString());        

        conf.setOutputKeyClass(Text.class);
        conf.setOutputValueClass(LongWritable.class);

        conf.setMapperClass(mapperClass);

        conf.setInputFormat(TextInputFormat.class);
        conf.setOutputFormat(TextOutputFormat.class);
        return conf;
    }

    /**
     * Generates a list of all the files contained within @param directoryPath
     * @param directoryPath The Hadoop Path to search for files in.
     * @throws java.io.IOException
     */
    public static List<FileStatus> getFileList(final FileSystem hdfs, final Path directoryPath) throws IOException {
        final ArrayList<FileStatus> fileList = new ArrayList<>();
        final FileStatus[] files = hdfs.listStatus(directoryPath);
        if(files.length != 0) {
            for (final FileStatus file: files) {
                if (file.isDirectory()) {
                    fileList.addAll(getFileList(hdfs, file.getPath()));
                }
                else {
                    fileList.add(file);
                }
            }
        }
        return fileList;
    }

    public static List<Ds3Object> convertFileStatusList(final List<FileStatus> fileList) {
        final List<Ds3Object> objectList = new ArrayList<>();

        for (final FileStatus file: fileList) {
            objectList.add(fileStatusToDs3Object(file));
        }
        return objectList;
    }

    static Ds3Object fileStatusToDs3Object(final FileStatus fileStatus) {
        final Ds3Object obj = new Ds3Object();
        try {
            obj.setName(PathUtils.stripPath(fileStatus.getPath().toString()));
        } catch (final URISyntaxException e) {
            System.err.println("The uri passed in was invalid.  This should not happen.");
        }
        obj.setSize(fileStatus.getLen());
        return obj;
    }
}
