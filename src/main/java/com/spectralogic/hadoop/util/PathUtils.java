package com.spectralogic.hadoop.util;


import com.spectralogic.ds3client.models.Ds3Object;
import org.apache.hadoop.fs.FileSystem;

import java.io.File;
import java.net.URI;
import java.net.URISyntaxException;

public class PathUtils {
    /**
     * Strips out the path from a uri.
     * @param path
     * @return
     * @throws URISyntaxException
     */
    public static String stripPath(final String path) throws URISyntaxException {
        final URI uri = new URI(path);
        return uri.getPath();
    }

    public static String join(final String path1, final String path2) {
        final File file1 = new File(path1);
        return new File(file1, path2).toString().replace("\\","/");
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
}
