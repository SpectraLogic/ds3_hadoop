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

package com.spectralogic.ds3.hadoop.util;

import com.spectralogic.ds3.hadoop.mappers.FileEntry;
import com.spectralogic.ds3client.models.bulk.BulkObject;
import com.spectralogic.ds3client.models.bulk.Ds3Object;
import com.spectralogic.ds3client.models.bulk.Objects;
import org.apache.hadoop.fs.*;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.List;

public class HdfsUtils {
    public static File writeToTemp(final List<Objects> masterObjectList) throws IOException {
        final File tempFile = File.createTempFile("migrator","dat");
        final PrintWriter writer = new PrintWriter(new FileWriter(tempFile));

        /* TODO: We will have to update this to take into account different nodes post 1.0. */
        for(final Objects objects: masterObjectList) {
            for(final BulkObject object: objects.getObjects()) {
                writer.printf("%s\n", FileEntry.fromBulkObject(object).toString());
            }
        }

        //flush the contents to make sure they are on disk before writing to hdfs
        writer.flush();
        writer.close();

        return tempFile;
    }

    /**
     * Generates a list of all the files contained within @param directoryPath
     * @param directoryPath The Hadoop Path to search for files in.
     * @throws java.io.IOException
     */
    public static List<FileStatus> getFileList(final FileSystem hdfs, final Path directoryPath) throws IOException {
        final ArrayList<FileStatus> fileList = new ArrayList<>();
        final FileStatus[] files = hdfs.listStatus(directoryPath);
        if(files.length != 0) {
            for (final FileStatus file: files) {
                if (file.isDirectory()) {
                    fileList.addAll(getFileList(hdfs, file.getPath()));
                }
                else {
                    fileList.add(file);
                }
            }
        }
        return fileList;
    }

    public static List<Ds3Object> convertFileStatusList(final List<FileStatus> fileList) {
        final List<Ds3Object> objectList = new ArrayList<>();

        for (final FileStatus file: fileList) {
            objectList.add(fileStatusToDs3Object(file));
        }
        return objectList;
    }

    static Ds3Object fileStatusToDs3Object(final FileStatus fileStatus) {
        final Ds3Object obj = new Ds3Object();
        try {
            obj.setName(PathUtils.stripPath(fileStatus.getPath().toString()));
        } catch (final URISyntaxException e) {
            System.err.println("The uri passed in was invalid.  This should not happen");
        }
        obj.setSize(fileStatus.getLen());
        return obj;
    }

    /**
     * Only delete the directory if it does not contain any other files.
     * @param outputPath
     */
    public static void safeDirectoryDelete(final FileSystem hdfs, final Path outputPath) throws IOException, UnsafeDeleteException {
        // Cleanup the result directory from any previous run
        if (hdfs.exists(outputPath)) {
            if (hdfs.isDirectory(outputPath)) {
                final RemoteIterator<LocatedFileStatus> iter = hdfs.listFiles(outputPath, true);
                while(iter.hasNext()) {
                    final LocatedFileStatus fileStatus = iter.next();
                    if (fileStatus.isFile() && fileStatus.getLen() != 0) {
                        throw new UnsafeDeleteException("The result directory contains a file: " + fileStatus.toString());
                    }
                }
                hdfs.delete(outputPath, true);
            }
            else {
                throw new UnsafeDeleteException("The output directory is pointing to a file and not a directory");
            }
        }
    }
}
