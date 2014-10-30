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

package com.spectralogic.ds3.hadoop.util;


import com.spectralogic.ds3.hadoop.mappers.FileEntry;
import com.spectralogic.ds3client.models.bulk.BulkObject;
import com.spectralogic.ds3client.models.bulk.Ds3Object;
import org.apache.hadoop.fs.FileSystem;

import java.io.File;
import java.net.URI;
import java.net.URISyntaxException;

public class PathUtils {
    /**
     * Returns the path from a uri.
     * @throws URISyntaxException
     */
    public static String stripPath(final String path) throws URISyntaxException {
        final URI uri = new URI(path);
        return uri.getPath();
    }

    public static String join(final String path1, final String path2) {
        if(path1 == null && path2 != null) {
            return path2;
        } else if (path2 == null && path1 != null) {
            return path1;
        } else if(path1 == null) {
            return "";
        }
        else {
            final File file1 = new File(path1);
            return new File(file1, path2).toString().replace("\\","/");
        }

    }

    public static String ensureStartsWithSlash(final String path) {
        if(!path.startsWith("/")) {
            return "/" + path;
        }
        return path;
    }

    public static boolean isDir(final Ds3Object obj) {
        return obj.getName().endsWith("/");
    }

    public static String getWorkingDirPath(final FileSystem fs) {
        return fs.getWorkingDirectory().toUri().getPath();
    }

    public static String removePrefixFromPath(final String path, final String name) {
        if (name.startsWith(path)) {
            return name.substring(path.length());
        }
        return name;
    }

    public static String objPath(final BulkObject obj) {
        return objPath(obj.getName(), obj.getOffset());
    }

    public static String objPath(final FileEntry fileEntry) {
        return objPath(fileEntry.getFileName(), fileEntry.getOffset());
    }

    private static String objPath(final String fileName, final long offset) {
        final String objName;
        if (offset == 0) {
            return fileName;
        }
        else {
            return fileName + "-" + Long.toString(offset);
        }

    }
}
