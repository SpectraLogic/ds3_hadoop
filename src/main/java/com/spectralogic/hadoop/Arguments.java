package com.spectralogic.hadoop;

import org.apache.commons.cli.*;
import org.apache.hadoop.conf.Configuration;


import java.util.ArrayList;
import java.util.List;

public class Arguments {

    private final Options options;
    private final Configuration configuration;

    private String bucket;
    private String srcDir;
    private String destDir;
    private String endpoint;
    private String accessKey;
    private String secretKey;
    private boolean secure = false;
    private Command command;
    private String prefix;

    public Arguments() {
        options = new Options();
        configuration = new Configuration();

        final Option ds3Endpoint = new Option("e", true, "The ds3 endpoint to connect to or have \"DS3_ENDPOINT\" set as an environment variable.");
        ds3Endpoint.setArgName("endpoint");
        final Option secure = new Option("s", false, "Set if the connection to the ds3 endpoint should be sent via https");
        final Option sourceDirectory = new Option("i", true, "The directory to copy to ds3");
        sourceDirectory.setArgName("directory");
        final Option destDirectory = new Option("o", true, "The output directory where any errors will be reported");
        destDirectory.setArgName("directory");
        final Option bucket = new Option("b", true, "The ds3 bucket to copy to");
        bucket.setArgName("bucket");
        final Option accessKey = new Option("a", true, "Access Key ID or have \"DS3_ACCESS_KEY\" set as an environment variable");
        accessKey.setArgName("accessKeyId");
        final Option secretKey = new Option("k", true, "Secret access key or have \"DS3_SECRET_KEY\" set as an environment variable");
        secretKey.setArgName("secretKey");
        final Option command = new Option("c", true, "What command you want to perform.  Can be: [put, get, buckets, objects, joblist]");
        command.setArgName("command");
        final Option prefix = new Option("p", true, "Specify a prefix to restore a bucket to.  This is an optional argument");
        command.setArgName("prefix");
        final Option help = new Option("h", "Print Help Menu");

        options.addOption(ds3Endpoint);
        options.addOption(secure);
        options.addOption(sourceDirectory);
        options.addOption(destDirectory);
        options.addOption(bucket);
        options.addOption(accessKey);
        options.addOption(secretKey);
        options.addOption(command);
        options.addOption(prefix);
        options.addOption(help);
    }

    protected Options getOptions() {
        return options;
    }

    protected void processCommandLine(final CommandLine cmd) throws MissingOptionException, BadArgumentException {
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

        this.setBucket(cmd.getOptionValue("b"));
        this.setDestDir(cmd.getOptionValue("o"));
        this.setSrcDir(cmd.getOptionValue("i"));
        this.setEndpoint(cmd.getOptionValue("e"));
        this.setAccessKey(cmd.getOptionValue("a"));
        this.setSecretKey(cmd.getOptionValue("k"));
        this.setPrefix(cmd.getOptionValue("p"));

        if (cmd.hasOption("s")) {
            this.setSecure(true);
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

        if (getBucket() == null && !bucketsCommand()) {
            missingArgs.add("b");
        }

        if (getDestDir() == null && !listCommand()) {
            missingArgs.add("o");
        }

        if (getSrcDir() == null && getCommand() == Command.PUT) {
            missingArgs.add("i");
        }

        return missingArgs;
    }

    private boolean bucketsCommand() {
        return getCommand() == Command.BUCKETS;

    }

    private boolean listCommand() {
        return getCommand() == Command.JOBS || getCommand() == Command.OBJECTS || bucketsCommand();
    }

    public void printHelp() {
        final HelpFormatter helpFormatter = new HelpFormatter();
        helpFormatter.printHelp("hdfs jar FileMigrator.jar", options);
    }

    public String getBucket() {
        return bucket;
    }

    private void setBucket(String bucket) {
        this.bucket = bucket;
    }

    public String getSrcDir() {
        return srcDir;
    }

    private void setSrcDir(String srcDir) {
        this.srcDir = srcDir;
    }

    public String getDestDir() {
        return destDir;
    }

    private void setDestDir(String destDir) {
        this.destDir = destDir;
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

    public boolean isSecure() {
        return secure;
    }

    private void setSecure(boolean secure) {
        this.secure = secure;
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
}
