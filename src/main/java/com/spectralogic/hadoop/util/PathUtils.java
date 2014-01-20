package com.spectralogic.hadoop.util;


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
}
