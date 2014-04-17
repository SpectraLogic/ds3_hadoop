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

package com.spectralogic.hadoop.util;

import com.spectralogic.ds3client.models.Ds3Object;
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

    @Test
    public void isDir() {
        final Ds3Object obj = new Ds3Object("test/", 0);

        assertThat(PathUtils.isDir(obj), is(true));
    }

    @Test
    public void isNotDir() {
        final Ds3Object obj = new Ds3Object("test", 0);

        assertThat(PathUtils.isDir(obj), is(false));
    }

    @Test
    public void isNotDirWithData() {
        final Ds3Object obj = new Ds3Object("test/", 12);

        assertThat(PathUtils.isDir(obj), is(true));
    }

    @Test
    public void removePrefixTest() {
        final String prefix = "/test/prefix";
        final String fullPath = "/test/prefix/my/path";

        assertThat(PathUtils.removePrefixFromPath(prefix, fullPath), is("/my/path"));
    }

    @Test
    public void bullTestPath() {
        assertThat(PathUtils.join(null, "path!"), is("path!"));
        assertThat(PathUtils.join("path!", null), is("path!"));
        assertThat(PathUtils.join(null, null), is(""));
    }

}
