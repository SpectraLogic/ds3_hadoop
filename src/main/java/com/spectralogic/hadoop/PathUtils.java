package com.spectralogic.hadoop;


import java.net.URI;
import java.net.URISyntaxException;

public class PathUtils {

    public static String stripPath(final String path) throws URISyntaxException {
        URI uri = new URI(path);
        return uri.getPath();
    }
}
