/*
 * ******************************************************************************
 *   Copyright 2014-2015 Spectra Logic Corporation. All Rights Reserved.
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

package com.spectralogic.ds3.hadoop.cli.commands;

import com.spectralogic.ds3.hadoop.cli.Arguments;
import com.spectralogic.ds3.hadoop.cli.Ds3Provider;
import org.apache.hadoop.fs.FileSystem;

import java.io.IOException;

public class ListJobsCommand extends AbstractCommand {

    public ListJobsCommand(final Ds3Provider provider, final FileSystem hdfsFileSystem) throws IOException {
        super(provider, hdfsFileSystem);
    }

    @Override
    public void init(final Arguments arguments) {

    }

    @Override
    public Boolean call() throws Exception {
        System.out.println("Operation Not Supported");
        return null;
    }
}
