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

package com.spectralogic.hadoop;

import com.spectralogic.hadoop.commands.*;
import org.apache.commons.cli.*;
import org.apache.hadoop.util.GenericOptionsParser;

import java.io.*;
import java.net.URISyntaxException;

public class FileMigrator {

    private final AbstractCommand command;

    public FileMigrator(final Arguments arguments) throws IOException, URISyntaxException {
        switch(arguments.getCommand()) {
            case PUT: {
                command = new PutCommand(arguments);
                break;
            }
            case GET: {
                command = new GetCommand(arguments);
                break;
            }
            case JOBS: {
                command = new JobListCommand(arguments);
                break;
            }
            case BUCKETS: {
                command = new BucketsCommand(arguments);
                break;
            }
            case OBJECTS:
            default: {
                command = new ObjectsCommand(arguments);
                break;
            }
        }
    }

    public void run() throws Exception {
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
            final FileMigrator migrator = new FileMigrator(arguments);

            migrator.run();
        }
        catch(final MissingOptionException e) {
            System.out.println("Error: " + e.getMessage());
            System.out.println("See the help command for a list of required arguments.");
        }
    }

}
