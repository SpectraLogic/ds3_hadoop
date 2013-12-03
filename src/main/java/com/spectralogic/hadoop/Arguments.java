package com.spectralogic.hadoop;

import org.apache.commons.cli.*;

import java.util.ArrayList;
import java.util.List;

public class Arguments {

    private final Options options;

    private String bucket;
    private String srcDir;
    private String destDir;
    private String endpoint;

    public Arguments() {
        options = new Options();

        final Option ds3Endpoint = new Option("e", true, "The ds3 endpoint");
        ds3Endpoint.setRequired(true);

        final Option sourceDirectory = new Option("i", true, "The directory to copy to ds3");
        sourceDirectory.setRequired(true);

        final Option destDirectory = new Option("o", true, "The output directory where any errors will be reported");
        sourceDirectory.setRequired(true);

        final Option bucket = new Option("b", true, "The ds3 bucket to copy to");
        bucket.setRequired(true);

        final Option help = new Option("h", "Print Help Menu");
        options.addOption(ds3Endpoint);
        options.addOption(sourceDirectory);
        options.addOption(destDirectory);
        options.addOption(bucket);
        options.addOption(help);

    }

    protected Options getOptions() {
        return options;
    }

    protected void processCommandLine(final CommandLine cmd) throws MissingOptionException {
        if(cmd.hasOption("h")) {
            printHelp();
            System.exit(0);
        }

        this.setBucket(cmd.getOptionValue("b"));
        this.setDestDir(cmd.getOptionValue("o"));
        this.setSrcDir(cmd.getOptionValue("i"));
        this.setEndpoint(cmd.getOptionValue("e"));

        final List<String> missingArgs = getMissingArgs();
        if(!missingArgs.isEmpty()) {
            throw new MissingOptionException(missingArgs);
        }
    }

    private List<String> getMissingArgs() {
        final List<String> missingArgs = new ArrayList<String>();

        if(getBucket() == null) {
            missingArgs.add("b");
        }

        if(getEndpoint() == null) {
            missingArgs.add("e");
        }

        if(getDestDir() == null) {
            missingArgs.add("o");
        }

        if(getSrcDir() == null) {
            missingArgs.add("i");
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

}
