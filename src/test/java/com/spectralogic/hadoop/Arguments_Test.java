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
    public void hasNoArgs() throws ParseException, IOException, BadArgumentException {
        final Arguments arguments = new Arguments();

        final String[] args = new String[0];
        final GenericOptionsParser optParser = new GenericOptionsParser(new Configuration(), arguments.getOptions(), args);
        final CommandLine cmd = optParser.getCommandLine();
        arguments.processCommandLine(cmd);
    }

    @Test
    public void hasArgs() throws ParseException, IOException, BadArgumentException {
        final Arguments arguments = new Arguments();

        final String[] args = new String[14];
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
        args[12] = "-c";
        args[13] = "put";

        final GenericOptionsParser optParser = new GenericOptionsParser(new Configuration(), arguments.getOptions(), args);
        final CommandLine cmd = optParser.getCommandLine();
        arguments.processCommandLine(cmd);

        assertThat(cmd.hasOption("b"), is(true));
        assertThat(cmd.hasOption("e"), is(true));
        assertThat(cmd.hasOption("o"), is(true));
        assertThat(cmd.hasOption("i"), is(true));
        assertThat(cmd.hasOption("c"), is(true));
    }

    @Test(expected = BadArgumentException.class)
    public void badCommand() throws ParseException, IOException, BadArgumentException {
        final Arguments arguments = new Arguments();

        final String[] args = new String[14];
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
        args[12] = "-c";
        args[13] = "badCommand";

        final GenericOptionsParser optParser = new GenericOptionsParser(new Configuration(), arguments.getOptions(), args);
        final CommandLine cmd = optParser.getCommandLine();
        arguments.processCommandLine(cmd);
    }

}
