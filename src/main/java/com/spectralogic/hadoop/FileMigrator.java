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
            case LIST:
            default: {
                command = new ListCommand(arguments);
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
        final Arguments arguments = processArgs(args);
        final FileMigrator migrator = new FileMigrator(arguments);

        migrator.run();
    }

}
