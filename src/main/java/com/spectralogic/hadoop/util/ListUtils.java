package com.spectralogic.hadoop.util;

import com.google.common.base.Predicate;
import com.google.common.collect.Iterators;
import com.spectralogic.ds3client.models.Ds3Object;

import java.util.Iterator;
import java.util.List;

public class ListUtils {
    public static Iterator<Ds3Object> filterDirectories(final List<Ds3Object> objects) {
        return Iterators.filter(objects.iterator(), new Predicate<Ds3Object>() {
            @Override
            public boolean apply(Ds3Object input) {
                return !PathUtils.isDir(input);
            }
        });
    }
}
