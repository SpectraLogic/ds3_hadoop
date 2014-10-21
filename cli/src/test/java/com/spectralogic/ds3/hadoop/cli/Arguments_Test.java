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

package com.spectralogic.ds3.hadoop.cli;

import org.apache.commons.cli.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.GenericOptionsParser;
import org.junit.Test;

import java.io.IOException;

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

    @Test
    public void fileList() throws ParseException, IOException, BadArgumentException {
        final Arguments arguments = new Arguments();

        final String[] args = new String[10];
        args[0] = "-b";
        args[1] = "bucketName";
        args[2] = "-e";
        args[3] = "http://localhost:8080";
        args[4] = "-k";
        args[5] = "MyKey";
        args[6] = "-a";
        args[7] = "accessId";
        args[8] = "-c";
        args[9] = "objects";

        final GenericOptionsParser optParser = new GenericOptionsParser(new Configuration(), arguments.getOptions(), args);
        final CommandLine cmd = optParser.getCommandLine();
        arguments.processCommandLine(cmd);

        assertThat(cmd.hasOption("b"), is(true));
        assertThat(cmd.hasOption("e"), is(true));
        assertThat(cmd.hasOption("o"), is(false));
        assertThat(cmd.hasOption("i"), is(false));
        assertThat(cmd.hasOption("c"), is(true));
    }

    @Test
    public void jobList() throws ParseException, IOException, BadArgumentException {
        final Arguments arguments = new Arguments();

        final String[] args = new String[10];
        args[0] = "-b";
        args[1] = "bucketName";
        args[2] = "-e";
        args[3] = "http://localhost:8080";
        args[4] = "-k";
        args[5] = "MyKey";
        args[6] = "-a";
        args[7] = "accessId";
        args[8] = "-c";
        args[9] = "jobs";

        final GenericOptionsParser optParser = new GenericOptionsParser(new Configuration(), arguments.getOptions(), args);
        final CommandLine cmd = optParser.getCommandLine();
        arguments.processCommandLine(cmd);

        assertThat(cmd.hasOption("b"), is(true));
        assertThat(cmd.hasOption("e"), is(true));
        assertThat(cmd.hasOption("o"), is(false));
        assertThat(cmd.hasOption("i"), is(false));
        assertThat(cmd.hasOption("c"), is(true));
    }

}
