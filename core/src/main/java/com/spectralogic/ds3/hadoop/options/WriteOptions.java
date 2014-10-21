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

public class WriteOptions {

    /**
     * Returns a new Default WriteOptions object with JobOutputDir set to '~/result' and hadoopTmpDir set to '/tmp/hadoop/ds3'
     */
    public static WriteOptions getDefault() {

        final WriteOptions defaultOptions = new WriteOptions();
        
        defaultOptions.setJobOutputDir("~/result");
        defaultOptions.setHadoopTmpDir("/tmp/hadoop/ds3");
        
        return defaultOptions;
    }

    private String hadoopTmpDir = null;
    private String jobOutputDir = null;

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

}