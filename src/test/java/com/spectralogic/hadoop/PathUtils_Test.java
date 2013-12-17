package com.spectralogic.hadoop;

import org.junit.Test;

import java.net.URISyntaxException;

import static org.hamcrest.CoreMatchers.*;
import static org.hamcrest.MatcherAssert.assertThat;

public class PathUtils_Test {


    @Test
    public void stripPath() throws URISyntaxException {
        final String path = "hdfs://ryan-hdfs:54310/user/hduser/gutenberg/20417.txt.utf-8";
        final String result = PathUtils.stripPath(path);

        assertThat(result, is("/user/hduser/gutenberg/20417.txt.utf-8"));
    }

    @Test
    public void joinPaths() {
        final String path = "/app/hadoop/tmp";
        final String name = "fileName";

        assertThat(PathUtils.join(path, name), is("/app/hadoop/tmp/fileName"));
    }
}
