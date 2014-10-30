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

package com.spectralogic.ds3.hadoop.mappers;

import com.spectralogic.ds3client.models.bulk.BulkObject;

public class FileEntry {
    private final String fileName;
    private final long offset;
    private final long length;

    public FileEntry (final String fileName, final long offset, final long length) {
        this.fileName = fileName;
        this.offset = offset;
        this.length = length;
    }

    public String getFileName() {
        return fileName;
    }

    public long getOffset() {
        return offset;
    }

    public long getLength() {
        return length;
    }

    public String toString() {
        return fileName + "," + Long.toString(offset) + "," + Long.toString(length);
    }

    public static FileEntry fromString(final String input) {
        final String[] fileDetails = input.split(",");
        final String fileName = fileDetails[0];
        final long offset = Long.parseLong(fileDetails[1]);
        final long length = Long.parseLong(fileDetails[2]);
        return new FileEntry(fileName, offset, length);
    }

    public static FileEntry fromBulkObject(final BulkObject bulkObject) {
        return new FileEntry(bulkObject.getName(), bulkObject.getOffset(), bulkObject.getLength());
    }
}
