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
        System.out.println("Operation Not Supported");
        return null;
    }
}
