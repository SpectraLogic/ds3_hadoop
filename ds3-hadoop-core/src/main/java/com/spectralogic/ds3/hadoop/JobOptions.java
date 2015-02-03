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

package com.spectralogic.ds3.hadoop;

public class JobOptions {

    public static JobOptions getDefault(final String tmpDir) {

        final JobOptions options = new JobOptions();
        options.setJobOutputDir("result");
        options.setHadoopTmpDir(tmpDir);

        return options;
    }

    private String hadoopTmpDir = null;
    private String jobOutputDir = null;
    private String prefix = null;

    public String getHadoopTmpDir() {
        return hadoopTmpDir;
    }

    public void setHadoopTmpDir(final String hadoopTmpDir) {
        this.hadoopTmpDir = hadoopTmpDir;
    }

    public String getJobOutputDir() {
        return jobOutputDir;
    }

    public void setJobOutputDir(final String jobOutputDir) {
        this.jobOutputDir = jobOutputDir;
    }

    public String getPrefix() {
        return prefix;
    }

    public void setPrefix(final String prefix) {
        this.prefix = prefix;
    }

}
