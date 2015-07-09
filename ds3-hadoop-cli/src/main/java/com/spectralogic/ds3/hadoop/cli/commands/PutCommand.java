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

import com.spectralogic.ds3.hadoop.Job;
import com.spectralogic.ds3.hadoop.JobOptions;
import com.spectralogic.ds3.hadoop.cli.Arguments;
import com.spectralogic.ds3.hadoop.cli.Ds3Provider;
import com.spectralogic.ds3.hadoop.util.HdfsUtils;
import com.spectralogic.ds3.hadoop.util.PathUtils;
import com.spectralogic.ds3client.models.bulk.Ds3Object;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.List;

public class PutCommand extends AbstractCommand {

    private String bucketName;
    private String directoryName;
    private String prefix;

    public PutCommand(final Ds3Provider provider, final FileSystem hdfsFileSystem) throws IOException {
        super(provider, hdfsFileSystem);
    }

    @Override
    public void init(final Arguments arguments) {
        this.bucketName = arguments.getBucket();
        if (this.bucketName == null) {
            throw new IllegalArgumentException("Error: Missing bucket name, please use '-b' to set the bucket name");
        }

        this.directoryName = arguments.getDirectory();
        if (this.directoryName == null) {
            throw new IllegalArgumentException("Error: Missing directory, please use '-d' to set the directory");
        }
        this.prefix = arguments.getPrefix();
    }

    @Override
    public Boolean call() throws Exception {

        final FileSystem hdfs = this.getHdfs();

        final String workingPath = PathUtils.getWorkingDirPath(getHdfs());

        final List<FileStatus> fileList = HdfsUtils.getFileList(hdfs, hdfs.resolvePath(new Path(this.directoryName)));

        final List<Ds3Object> objects = toDs3Objects(fileList, workingPath, this.prefix);

        this.getDs3ClientHelpers().ensureBucketExists(this.bucketName);

        final Job job = this.getDs3HadoopHelper().startWriteJob(bucketName, objects, JobOptions.getDefault("/tmp"));

        job.transfer();

        return true;
    }

    private static List<Ds3Object> toDs3Objects(final List<FileStatus> fileList, final String workingPath, final String prefix) throws URISyntaxException {
        final List<Ds3Object> objects = new ArrayList<>(fileList.size());

        for (final FileStatus fileStatus : fileList) {
            String name = PathUtils.stripPath(fileStatus.getPath().toString());
            name = PathUtils.removePrefixFromPath(workingPath + "/", name);

            if (prefix != null) {
                name = prefix + name;
            }

            final Ds3Object ds3Object = new Ds3Object(name, fileStatus.getLen());
            objects.add(ds3Object);
        }
        return objects;
    }

}
