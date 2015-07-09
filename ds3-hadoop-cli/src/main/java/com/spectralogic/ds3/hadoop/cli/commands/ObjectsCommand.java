/*
 * ******************************************************************************
 *   Copyright 2014-2015 Spectra Logic Corporation. All Rights Reserved.
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

package com.spectralogic.ds3.hadoop.cli.commands;


import com.bethecoder.ascii_table.ASCIITable;
import com.bethecoder.ascii_table.ASCIITableHeader;
import com.spectralogic.ds3.hadoop.cli.Ds3Provider;
import com.spectralogic.ds3client.models.Contents;
import com.spectralogic.ds3client.networking.FailedRequestException;
import com.spectralogic.ds3.hadoop.cli.Arguments;
import org.apache.hadoop.fs.FileSystem;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;

public class ObjectsCommand extends AbstractCommand {

    private String bucketName;

    public ObjectsCommand(final Ds3Provider provider, final FileSystem hdfsFileSystem) throws IOException {
        super(provider, hdfsFileSystem);
    }

    @Override
    public Boolean call() throws Exception {
        try {

            final Iterable<Contents> objects = this.getDs3ClientHelpers().listObjects(this.bucketName);
            final Iterator<Contents> objIter = objects.iterator();
            if(objIter.hasNext()) {
                ASCIITable.getInstance().printTable(getHeaders(), formatBucketList(objIter));
            }
            else {
                System.out.println("No objects were reported in the bucket.");
            }
        }
        catch(final FailedRequestException e) {
            if(e.getStatusCode() == 500) {
                System.out.println("Error: Cannot communicate with the remote DS3 appliance.");
            }
            else if(e.getStatusCode() == 404) {
                System.out.println("Error: Unknown bucket.");
            }
            else {
                System.out.println("Error: Encountered an unknown error of ("+ e.getStatusCode() +") while accessing the remote DS3 appliance.");
            }
        }
        return true;
    }

    private String[][] formatBucketList(final Iterator<Contents> objects) {
        final ArrayList<String[]> formatArray = new ArrayList<>();
        Contents content;
        while(objects.hasNext()) {

            content = objects.next();
            final String[] arrayEntry = new String[5];
            arrayEntry[0] = content.getKey();
            arrayEntry[1] = Long.toString(content.getSize());
            arrayEntry[2] = content.getOwner().getDisplayName();
            arrayEntry[3] = nullGuard(content.getLastModified());
            arrayEntry[4] = nullGuard(content.geteTag());
            formatArray.add(arrayEntry);
        }

        return formatArray.toArray(new String[formatArray.size()][]);
    }

    private ASCIITableHeader[] getHeaders() {
        return new ASCIITableHeader[]{
                new ASCIITableHeader("File Name", ASCIITable.ALIGN_LEFT),
                new ASCIITableHeader("Size", ASCIITable.ALIGN_RIGHT),
                new ASCIITableHeader("Owner", ASCIITable.ALIGN_RIGHT),
                new ASCIITableHeader("Last Modified", ASCIITable.ALIGN_LEFT),
                new ASCIITableHeader("ETag", ASCIITable.ALIGN_RIGHT)};

    }

    private String nullGuard(String message) {
        if(message == null) {
            return "N/A";
        }
        return message;
    }

    @Override
    public void init(final Arguments arguments) {
        this.bucketName = arguments.getBucket();
        if (this.bucketName == null) {
            throw new IllegalArgumentException("Error: Missing bucket name.  Please use '-b' to set the bucket name");
        }
    }
}
