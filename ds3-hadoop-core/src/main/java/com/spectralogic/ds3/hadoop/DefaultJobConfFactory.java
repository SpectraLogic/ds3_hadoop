package com.spectralogic.ds3.hadoop;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapred.JobConf;

class DefaultJobConfFactory extends AbstractJobConfFactory {

    @Override
    public JobConf createNewJobConf(final Configuration baseConfig) {
        final JobConf jobConf = new JobConf(baseConfig);
        jobConf.setJobName(Constants.JOB_NAME + this.getIterationCount());
        return jobConf;
    }
}
