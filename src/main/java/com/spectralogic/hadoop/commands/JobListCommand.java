package com.spectralogic.hadoop.commands;

import com.spectralogic.hadoop.Arguments;
import org.apache.hadoop.mapred.JobConf;

import java.io.IOException;

public class JobListCommand extends AbstractCommand {
    public JobListCommand(Arguments arguments) throws IOException {
        super(arguments);
    }

    @Override
    public void init(JobConf conf) {
        //Pass
    }

    @Override
    public Boolean call() throws Exception {
        return null;
    }
}
