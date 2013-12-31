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
    private int port = 8080;
    private boolean secure = false;
    private Command command;

    public Arguments() {
        options = new Options();
        configuration = new Configuration();

        final Option ds3Endpoint = new Option("e", true, "The ds3 endpoint");
        ds3Endpoint.setArgName("url");
        final Option secure = new Option("s", false, "Set if the connection to the ds3 endpoint should be sent via https");
        final Option port = new Option("p", true, "Set the port to connect to ds3 on (default: 8080)");
        port.setArgName("port");
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
        final Option command = new Option("c", true, "What command you want to perform.  Can be: [put, get]");
        secretKey.setArgName("command");
        final Option help = new Option("h", "Print Help Menu");

        options.addOption(ds3Endpoint);
        options.addOption(secure);
        options.addOption(port);
        options.addOption(sourceDirectory);
        options.addOption(destDirectory);
        options.addOption(bucket);
        options.addOption(accessKey);
        options.addOption(secretKey);
        options.addOption(command);
        options.addOption(help);

    }

    protected Options getOptions() {
        return options;
    }

    protected void processCommandLine(final CommandLine cmd) throws MissingOptionException, BadArgumentException {
        if(cmd.hasOption('h')) {
            printHelp();
            System.exit(0);
        }

        try {
            final String commandString = cmd.getOptionValue("c");
            if(commandString == null) {
                this.setCommand(null);
            }
            else {
                this.setCommand(Command.valueOf(commandString.toUpperCase()));
            }
        }
        catch (IllegalArgumentException e) {
            throw new BadArgumentException("Unknown command", e);
        }

        this.setBucket(cmd.getOptionValue("b"));
        this.setDestDir(cmd.getOptionValue("o"));
        this.setSrcDir(cmd.getOptionValue("i"));
        this.setEndpoint(cmd.getOptionValue("e"));
        this.setAccessKey(cmd.getOptionValue("a"));
        this.setSecretKey(cmd.getOptionValue("k"));
        if(cmd.hasOption("p")) {
            this.setPort(Integer.valueOf(cmd.getOptionValue("p")));
        }
        if(cmd.hasOption("s")) {
            this.setSecure(true);
        }

        final List<String> missingArgs = getMissingArgs();

        if(getSecretKey() == null) {
            final String key = System.getenv("DS3_SECRET_KEY");
            if(key == null) {
                missingArgs.add("k");
            }
        }

        if(getAccessKey() == null) {
            final String key = System.getenv("DS3_ACCESS_KEY");
            if(key == null) {
                missingArgs.add("a");
            }
        }

        if(!missingArgs.isEmpty()) {
            throw new MissingOptionException(missingArgs);
        }
    }

    private List<String> getMissingArgs() {
        final List<String> missingArgs = new ArrayList<String>();

        if (getBucket() == null) {
            missingArgs.add("b");
        }

        if (getEndpoint() == null) {
            missingArgs.add("e");
        }

        if (getDestDir() == null) {
            missingArgs.add("o");
        }

        if (getSrcDir() == null) {
            missingArgs.add("i");
        }

        if (getCommand() == null) {
            missingArgs.add("c");
        }

        return missingArgs;
    }

    public void printHelp() {
        final HelpFormatter helpFormatter = new HelpFormatter();
        helpFormatter.printHelp("hdfs -jar FileMigrator.jar", options);
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

    public int getPort() {
        return port;
    }

    private void setPort(int port) {
        this.port = port;
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
}
