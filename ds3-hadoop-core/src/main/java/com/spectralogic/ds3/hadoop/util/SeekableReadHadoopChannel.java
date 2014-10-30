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

import org.apache.hadoop.fs.FSDataInputStream;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SeekableByteChannel;

public class SeekableReadHadoopChannel implements SeekableByteChannel {
    private final FSDataInputStream stream;
    private final long length;
    private final long offset;
    private final byte[] buffer;

    private long position;
    private boolean open = true;

    public SeekableReadHadoopChannel(final FSDataInputStream stream, final long length, final long offset) {
        this.stream = stream;
        this.length = length;
        this.offset = offset;
        this.position = offset;
        this.buffer = new byte[1024 * 1024];
    }

    @Override
    public int read(final ByteBuffer dst) throws IOException {
        final int bytesRead = stream.read(buffer);
        dst.put(buffer, 0, bytesRead);
        return bytesRead;
    }

    @Override
    public int write(final ByteBuffer src) throws IOException {
        throw new UnsupportedOperationException();
    }

    @Override
    public long position() throws IOException {
        return position;
    }

    @Override
    public SeekableByteChannel position(final long newPosition) throws IOException {
        final long seekTo;
        if (newPosition < offset) {
            seekTo = offset;
        }
        else if (newPosition > length + offset) {
            seekTo = length + offset;
        }
        else {
            seekTo = newPosition;
        }
        this.stream.seek(seekTo);
        return this;
    }

    @Override
    public long size() throws IOException {
        return length;
    }

    @Override
    public SeekableByteChannel truncate(final long size) throws IOException {
        throw new UnsupportedOperationException("This operation is currently not supported.");
    }

    @Override
    public boolean isOpen() {
        return this.open;
    }

    @Override
    public void close() throws IOException {
        this.stream.close();
        this.open = false;
    }

    public static SeekableByteChannel wrap(final FSDataInputStream stream, final long length, final long offset) {
        return new SeekableReadHadoopChannel(stream, length, offset);
    }
}
