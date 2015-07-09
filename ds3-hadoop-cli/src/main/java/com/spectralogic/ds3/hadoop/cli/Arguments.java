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

import org.apache.commons.cli.*;
import org.apache.hadoop.conf.Configuration;


import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.List;

public class Arguments {

    private final Options options;
    private final Configuration configuration;

    private String bucket;
    private String directory;
    private String errorDirectory;
    private String endpoint;
    private String accessKey;
    private String secretKey;
    private boolean certificateVerification = true;
    private boolean https = true;
    private Command command;
    private String prefix;
    private int retries = 5;

    public Arguments() {
        options = new Options();
        configuration = new Configuration(); // if ran in the hadoop cluster than the configuration does not need to be
                                             // modified

        final Option ds3Endpoint = new Option("e", true, "The ds3 endpoint to connect to or have \"DS3_ENDPOINT\" set as an environment variable.");
        ds3Endpoint.setArgName("endpoint");
        final Option certificateVerification = new Option(null, false, "Set if the connection to the ds3 endpoint should be sent via https");
        certificateVerification.setLongOpt("insecure");
        final Option https = new Option(null, false, "Set if the connection should be sent over standard http and not https");
        https.setLongOpt("http");
        final Option directory = new Option("d", true, "The directory to copy to ds3 or from ds3");
        directory.setArgName("directory");
        final Option errorDirectory = new Option(null, true, "The output directory where any errors will be reported, creates a temporary directory in the tmp directory if not set");
        errorDirectory.setLongOpt("errors");
        final Option bucket = new Option("b", true, "The ds3 bucket to copy to");
        bucket.setArgName("bucket");
        final Option accessKey = new Option("a", true, "Access Key ID or have \"DS3_ACCESS_KEY\" set as an environment variable");
        accessKey.setArgName("accessKeyId");
        final Option secretKey = new Option("k", true, "Secret access key or have \"DS3_SECRET_KEY\" set as an environment variable");
        secretKey.setArgName("secretKey");
        final Option command = new Option("c", true, "What command you want to perform.  Can be: [put, get, buckets, objects, joblist]");
        command.setArgName("command");
        final Option prefix = new Option("p", true, "Specify a prefix to restore a bucket to.  This is an optional argument");
        prefix.setArgName("prefix");
        final Option redirectRetries = new Option("r", true, "Specify how many time a mapper should retry after receiving a 307 redirect.  Defaults to 5");
        redirectRetries.setArgName("redirectRetries");
        final Option help = new Option("h", "Print Help Menu");

        options.addOption(ds3Endpoint);
        options.addOption(certificateVerification);
        options.addOption(directory);
        options.addOption(https);
        options.addOption(errorDirectory);
        options.addOption(bucket);
        options.addOption(accessKey);
        options.addOption(secretKey);
        options.addOption(command);
        options.addOption(prefix);
        options.addOption(redirectRetries);
        options.addOption(help);
    }

    Options getOptions() {
        return options;
    }

    void processCommandLine(final CommandLine cmd) throws MissingOptionException, BadArgumentException {
        if (cmd.hasOption('h')) {
            printHelp();
            System.exit(0);
        }

        try {
            final String commandString = cmd.getOptionValue("c");
            if (commandString == null) {
                this.setCommand(null);
            } else {
                this.setCommand(Command.valueOf(commandString.toUpperCase()));
            }
        } catch (IllegalArgumentException e) {
            throw new BadArgumentException("Unknown command", e);
        }


        if (cmd.hasOption("http")) {
            this.setHttps(false);
        }

        if (cmd.hasOption("insecure")) {
            this.setCertificateVerification(false);
        }

        this.setDirectory(cmd.getOptionValue("d"));
        this.setBucket(cmd.getOptionValue("b"));
        this.setEndpoint(cmd.getOptionValue("e"));
        this.setAccessKey(cmd.getOptionValue("a"));
        this.setSecretKey(cmd.getOptionValue("k"));
        this.setPrefix(cmd.getOptionValue("p"));
        this.setErrorDirectory(cmd.getOptionValue("errors"));

        if(cmd.hasOption("r")) {
            try {
                this.setRedirectRetries(Integer.valueOf(cmd.getOptionValue("r")));
            }
            catch (final NumberFormatException e) {
                throw new BadArgumentException("Redirect Retries must be an integer", e);
            }
        }

        if (cmd.hasOption("s")) {
            this.setCertificateVerification(true);
        }

        final List<String> missingArgs = getMissingArgs();

        if (getEndpoint() == null) {
            final String endpoint = System.getenv("DS3_ENDPOINT");
            if (endpoint == null) {
                missingArgs.add("e");
            } else {
                setEndpoint(endpoint);
            }
        }

        if (getSecretKey() == null) {
            final String key = System.getenv("DS3_SECRET_KEY");
            if (key == null) {
                missingArgs.add("k");
            } else {
                setSecretKey(key);
            }
        }

        if (getAccessKey() == null) {
            final String key = System.getenv("DS3_ACCESS_KEY");
            if (key == null) {
                missingArgs.add("a");
            } else {
                setAccessKey(key);
            }
        }

        if (!missingArgs.isEmpty()) {
            throw new MissingOptionException(missingArgs);
        }
    }

    private List<String> getMissingArgs() {
        final List<String> missingArgs = new ArrayList<>();

        if (getCommand() == null) {
            missingArgs.add("c");
        }

        return missingArgs;
    }

    void printHelp() {
        final HelpFormatter helpFormatter = new HelpFormatter();
        helpFormatter.printHelp("hdfs jar FileMigrator.jar", options);
    }

    public String getBucket() {
        return bucket;
    }

    private void setBucket(String bucket) {
        this.bucket = bucket;
    }

    public String getEndpoint() {
        return endpoint;
    }

    private void setEndpoint(String endpoint) {
        this.endpoint = endpoint;
    }

    public String getAccessKey() {
        return accessKey;
    }

    private void setAccessKey(String accessKey) {
        this.accessKey = accessKey;
    }

    public String getSecretKey() {
        return secretKey;
    }

    private void setSecretKey(String secretKey) {
        this.secretKey = secretKey;
    }

    public Configuration getConfiguration() {
        return configuration;
    }

    public boolean isCertificateVerification() {
        return certificateVerification;
    }

    private void setCertificateVerification(boolean certificateVerification) {
        this.certificateVerification = certificateVerification;
    }

    public Command getCommand() {
        return command;
    }

    private void setCommand(Command command) {
        this.command = command;
    }

    private void setPrefix(final String prefix) {
        this.prefix = prefix;
    }

    public String getPrefix() {
        return prefix;
    }

    private void setRedirectRetries(final int retries) {
        this.retries = retries;
    }

    public int getRedirectRetries() {
        return retries;
    }

    public boolean isHttps() {
        return https;
    }

    private void setHttps(final boolean https) {
        this.https = https;
    }

    public String getDirectory() {
        return directory;
    }

    void setDirectory(final String directory) {
        this.directory = directory;
    }

    public String getErrorDirectory() {
        return errorDirectory;
    }

    void setErrorDirectory(final String errorDirectory) {
        this.errorDirectory = errorDirectory;
    }
}
