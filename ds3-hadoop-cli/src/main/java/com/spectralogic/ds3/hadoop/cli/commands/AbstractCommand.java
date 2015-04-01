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

package com.spectralogic.ds3.hadoop.cli.commands;

import com.spectralogic.ds3client.Ds3Client;
import com.spectralogic.ds3client.Ds3ClientBuilder;
import com.spectralogic.ds3client.models.*;
import com.spectralogic.ds3.hadoop.cli.Arguments;
import org.apache.hadoop.fs.FileSystem;

import java.io.IOException;
import java.net.URI;
import java.util.concurrent.Callable;

public abstract class AbstractCommand implements Callable<Boolean> {

    private final FileSystem hdfs;
    private final Ds3Client ds3Client;
    private final String bucket;

    private final String inputDirectory;
    private final String outputDirectory;
    private URI jobTracker;

    AbstractCommand(final Arguments arguments) throws IOException {
        final Ds3ClientBuilder builder = Ds3ClientBuilder.create(arguments.getEndpoint(), new Credentials(arguments.getAccessKey(), arguments.getSecretKey()));
        this.ds3Client = builder.withCertificateVerification(arguments.isCertificateVerification())
                .withRedirectRetries(arguments.getRedirectRetries())
                .withHttps(arguments.isHttps())
                .build();

        //These args should only be null on list commands.
        if(arguments.getSrcDir() != null) {
            this.inputDirectory = arguments.getSrcDir();
        }
        else {
            this.inputDirectory = null;
        }
        if(arguments.getDestDir() != null) {
            this.outputDirectory = arguments.getDestDir();
        }
        else {
            this.outputDirectory = null;
        }

        this.jobTracker = arguments.getJobTracker();

        this.bucket = arguments.getBucket();

        this.hdfs = FileSystem.get(arguments.getConfiguration());
    }

    FileSystem getHdfs() {
        return hdfs;
    }

    String getBucket() {
        return bucket;
    }

    Ds3Client getDs3Client() {
        return ds3Client;
    }

    String getInputDirectory() {
        return inputDirectory;
    }

    String getOutputDirectory() {
        return outputDirectory;
    }

    URI getJobTracker() {

        return jobTracker;
    }
}
