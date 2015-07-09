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

package com.spectralogic.ds3.hadoop.cli;

import com.spectralogic.ds3.hadoop.Ds3HadoopHelper;
import com.spectralogic.ds3.hadoop.cli.commands.*;
import com.spectralogic.ds3client.Ds3Client;
import com.spectralogic.ds3client.Ds3ClientBuilder;
import com.spectralogic.ds3client.helpers.Ds3ClientHelpers;
import com.spectralogic.ds3client.models.Credentials;
import org.apache.commons.cli.*;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.fs.FileSystem;

import java.io.*;

public class FileMigrator {

    private final AbstractCommand command;
    private final Arguments arguments;

    public FileMigrator(final Arguments arguments, final Ds3Provider provider, final FileSystem hdfs) throws IOException {
        this.arguments = arguments;
        this.command = getCommand(arguments.getCommand(), provider, hdfs);
    }

    private static AbstractCommand getCommand(final Command commandType, final Ds3Provider provider, final FileSystem hdfs) throws IOException {
        switch(commandType) {
            case PUT: {
                return new PutCommand(provider, hdfs);
            }
            case GET: {
                return new GetCommand(provider, hdfs);
            }
            case JOBS: {
                return new ListJobsCommand(provider, hdfs);
            }
            case BUCKETS: {
                return new BucketsCommand(provider, hdfs);
            }
            case OBJECTS:
            default: {
                return new ObjectsCommand(provider, hdfs);
            }
        }
    }

    void run() throws Exception {
        command.init(arguments);
        command.call();
    }

    private static Arguments processArgs(final String args[]) throws IOException, MissingOptionException, BadArgumentException {
        final Arguments arguments = new Arguments();
        final Options options = arguments.getOptions();
        final GenericOptionsParser optParser = new GenericOptionsParser(arguments.getConfiguration(), options, args);

        arguments.processCommandLine(optParser.getCommandLine());

        return arguments;
    }

    public static void main(final String args[]) throws Exception {
        try {
            final Arguments arguments = processArgs(args);
            final Ds3Client client = createClient(arguments);

            final FileSystem hdfs = FileSystem.get(arguments.getConfiguration());
            final Ds3Provider provider = new Ds3Provider(client, Ds3ClientHelpers.wrap(client), Ds3HadoopHelper.wrap(client, hdfs, arguments.getConfiguration()));
            final FileMigrator migrator = new FileMigrator(arguments, provider, hdfs);

            migrator.run();
        }
        catch(final MissingOptionException e) {
            System.out.println("Error: " + e.getMessage());
            System.out.println("See the help command for a list of required arguments.");
        }
    }

    private static Ds3Client createClient(final Arguments args) {
        final Ds3ClientBuilder builder = Ds3ClientBuilder.create(args.getEndpoint(), new Credentials(args.getAccessKey(), args.getSecretKey()));
        builder.withHttps(args.isHttps())
                .withCertificateVerification(args.isCertificateVerification())
                .withRedirectRetries(args.getRedirectRetries());

        //TODO need to add proxy support

        return builder.build();
    }

}
