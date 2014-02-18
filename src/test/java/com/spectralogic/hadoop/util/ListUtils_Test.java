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
