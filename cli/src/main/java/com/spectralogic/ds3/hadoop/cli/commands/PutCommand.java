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

package com.spectralogic.ds3.hadoop.cli.commands;

import com.google.common.base.Function;
import com.google.common.collect.Lists;
import com.spectralogic.ds3.hadoop.HadoopHelper;
import com.spectralogic.ds3.hadoop.Job;
import com.spectralogic.ds3.hadoop.cli.Arguments;
import com.spectralogic.ds3.hadoop.options.HadoopOptions;
import com.spectralogic.ds3.hadoop.options.WriteOptions;
import com.spectralogic.ds3.hadoop.util.HdfsUtils;
import com.spectralogic.ds3.hadoop.util.PathUtils;
import com.spectralogic.ds3client.models.bulk.Ds3Object;
import com.spectralogic.ds3client.serializer.XmlProcessingException;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.URI;
import java.security.SignatureException;
import java.util.List;

public class PutCommand extends AbstractCommand {

    public PutCommand(final Arguments args) throws IOException {
        super(args);
    }

    @Override
    public Boolean call() throws SignatureException, IOException, XmlProcessingException {

        final List<FileStatus> fileList = HdfsUtils.getFileList(getHdfs(), new Path(getInputDirectory()));
        final List<Ds3Object> objectList = Lists.transform(HdfsUtils.convertFileStatusList(fileList), new Function<Ds3Object, Ds3Object>() {
            @Override
            public Ds3Object apply(final Ds3Object input) {
                final String path = PathUtils.getWorkingDirPath(getHdfs());
                final String newName = PathUtils.removePrefixFromPath(path, input.getName());
                input.setName(newName);
                return input;
            }
        });

        final HadoopHelper helper = HadoopHelper.wrap(getDs3Client(), getHdfs(), getHadoopOptions());

        final Job job = helper.startWriteJob(getBucket(), objectList, WriteOptions.getDefault());

        job.transfer();

        return true;
    }

    private HadoopOptions getHadoopOptions() {
        final URI jobTracker = getJobTracker();

        final HadoopOptions hadoopOptions = HadoopOptions.getDefaultOptions();

        hadoopOptions.setJobTracker(new InetSocketAddress(jobTracker.getHost(), jobTracker.getPort()));
        return hadoopOptions;
    }




}
