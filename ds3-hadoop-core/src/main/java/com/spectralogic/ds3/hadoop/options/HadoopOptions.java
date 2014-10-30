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

package com.spectralogic.ds3.hadoop.options;

import com.spectralogic.ds3.hadoop.Constants;
import org.apache.hadoop.conf.Configuration;

import java.net.InetSocketAddress;

public class HadoopOptions {

    private Configuration config;
    private InetSocketAddress jobTracker;

    public static HadoopOptions getDefaultOptions() {
        final HadoopOptions hadoopOptions = new HadoopOptions();
        hadoopOptions.setConfig(getDefaultConfiguration());
        hadoopOptions.setJobTracker(new InetSocketAddress("localhost", 50030));
        return hadoopOptions;
    }

    public static Configuration getDefaultConfiguration() {
        final Configuration conf = new Configuration();
        conf.set(Constants.MAPREDUCE_FRAMEWORK_NAME, "yarn");
        return conf;
    }

    public Configuration getConfig() {
        return config;
    }

    public void setConfig(final Configuration config) {
        this.config = config;
    }

    public InetSocketAddress getJobTracker() {
        return jobTracker;
    }

    public void setJobTracker(final InetSocketAddress jobTracker) {
        this.jobTracker = jobTracker;
    }
}
