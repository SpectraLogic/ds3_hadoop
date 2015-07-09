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

import com.spectralogic.ds3.hadoop.Ds3HadoopHelper;
import com.spectralogic.ds3.hadoop.cli.Ds3Provider;
import com.spectralogic.ds3client.Ds3Client;
import com.spectralogic.ds3.hadoop.cli.Arguments;
import com.spectralogic.ds3client.helpers.Ds3ClientHelpers;
import org.apache.hadoop.fs.FileSystem;

import java.io.IOException;
import java.util.concurrent.Callable;

public abstract class AbstractCommand implements Callable<Boolean> {

    private final FileSystem hdfs;
    private final Ds3Provider provider;

    AbstractCommand(final Ds3Provider provider, final FileSystem hdfsFileSystem) throws IOException {
        this.provider = provider;
        this.hdfs = hdfsFileSystem;
    }

    FileSystem getHdfs() {
        return hdfs;
    }

    Ds3Client getDs3Client() {
        return provider.getClient();
    }

    Ds3ClientHelpers getDs3ClientHelpers() {
        return provider.getHelpers();
    }

    Ds3HadoopHelper getDs3HadoopHelper() {
        return provider.getHadoopHelper();
    }

    public abstract void init(final Arguments arguments);
}
