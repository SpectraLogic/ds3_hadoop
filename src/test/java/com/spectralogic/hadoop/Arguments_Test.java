package com.spectralogic.hadoop;

import org.apache.commons.cli.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.GenericOptionsParser;
import org.junit.Test;

import java.io.IOException;
import java.net.URISyntaxException;

import static org.hamcrest.CoreMatchers.*;
import static org.hamcrest.MatcherAssert.assertThat;

public class Arguments_Test {

    @Test(expected = MissingOptionException.class)
    public void hasNoArgs() throws ParseException, IOException {
        final Arguments arguments = new Arguments();

        final String[] args = new String[0];
        final GenericOptionsParser optParser = new GenericOptionsParser(new Configuration(), arguments.getOptions(), args);
        final CommandLine cmd = optParser.getCommandLine();
        arguments.processCommandLine(cmd);
    }

    @Test
    public void hasArgs() throws ParseException, IOException {
        final Arguments arguments = new Arguments();
        final Parser parser = new BasicParser();

        final String[] args = new String[12];
        args[0] = "-b";
        args[1] = "bucketName!";
        args[2] = "-e";
        args[3] = "http://localhost:8080";
        args[4] = "-i";
        args[5] = "/users/user/directory";
        args[6] = "-o";
        args[7] = "/users/user/output";
        args[8] = "-k";
        args[9] = "MyKey";
        args[10] = "-a";
        args[11] = "accessId";

        final GenericOptionsParser optParser = new GenericOptionsParser(new Configuration(), arguments.getOptions(), args);
        final CommandLine cmd = optParser.getCommandLine();
        arguments.processCommandLine(cmd);

        assertThat(cmd.hasOption("b"), is(true));
        assertThat(cmd.hasOption("e"), is(true));
        assertThat(cmd.hasOption("o"), is(true));
        assertThat(cmd.hasOption("i"), is(true));
    }

    @Test
    public void stripPath() throws URISyntaxException {
        final String path = "hdfs://ryan-hdfs:54310/user/hduser/gutenberg/20417.txt.utf-8";
        final String result = PathUtils.stripPath(path);

        assertThat(result, is("/user/hduser/gutenberg/20417.txt.utf-8"));
    }
}
