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

import com.google.common.base.Predicate;
import com.google.common.collect.Iterators;
import com.spectralogic.ds3client.models.bulk.Ds3Object;

import java.util.Iterator;
import java.util.List;

public class ListUtils {
    public static Iterator<Ds3Object> filterDirectories(final List<Ds3Object> objects) {
        return Iterators.filter(objects.iterator(), new Predicate<Ds3Object>() {
            @Override
            public boolean apply(final Ds3Object input) {
                return !PathUtils.isDir(input);
            }
        });
    }
}
