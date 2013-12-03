package com.spectralogic.hadoop;

import org.apache.commons.cli.*;
import org.junit.Test;

import static org.hamcrest.CoreMatchers.*;
import static org.hamcrest.MatcherAssert.assertThat;

public class Arguments_Test {

    @Test(expected = MissingOptionException.class)
    public void hasNoArgs() throws ParseException {
        final Arguments arguments = new Arguments();

        final Parser parser = new BasicParser();
        parser.parse(arguments.getOptions(), new String[0]);
    }

    @Test
    public void hasArgs() throws ParseException {
        final Arguments arguments = new Arguments();
        final Parser parser = new BasicParser();

        final String[] args = new String[8];
        args[0] = "-b";
        args[1] = "bucketName!";
        args[2] = "-e";
        args[3] = "http://localhost:8080";
        args[4] = "-i";
        args[5] = "/users/user/directory";
        args[6] = "-o";
        args[7] = "/users/user/output";

        final CommandLine cmd = parser.parse(arguments.getOptions(), args);
        arguments.processCommandLine(cmd);

        assertThat(cmd.hasOption("b"), is(true));
        assertThat(cmd.hasOption("e"), is(true));
        assertThat(cmd.hasOption("o"), is(true));
        assertThat(cmd.hasOption("i"), is(true));
    }
}
