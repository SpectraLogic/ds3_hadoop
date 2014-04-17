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

import com.google.common.collect.Lists;
import com.spectralogic.ds3client.models.Ds3Object;
import org.junit.Test;


import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import static org.hamcrest.CoreMatchers.*;
import static org.hamcrest.MatcherAssert.assertThat;

public class ListUtils_Test {
    @Test
    public void testFilter() {
        final List<Ds3Object> objects = new ArrayList<Ds3Object>();
        objects.add(new Ds3Object("hi", 3));
        objects.add(new Ds3Object("hiAgain", 12));
        objects.add(new Ds3Object("folder/", 0));

        final Iterator<Ds3Object> iterator = ListUtils.filterDirectories(objects);
        final List<Ds3Object> finalList = Lists.newArrayList(iterator);

        assertThat(finalList.size(), is(2));
    }
}
