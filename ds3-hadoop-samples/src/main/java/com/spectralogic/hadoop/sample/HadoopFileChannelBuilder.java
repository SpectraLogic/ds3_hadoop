package com.spectralogic.hadoop.sample;

import com.spectralogic.ds3client.helpers.Ds3ClientHelpers;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SeekableByteChannel;

public class HadoopFileChannelBuilder implements Ds3ClientHelpers.ObjectChannelBuilder {
    private final FileSystem hdfs;

    public HadoopFileChannelBuilder(final FileSystem hdfs) {
        this.hdfs = hdfs;
    }

    @Override
    public SeekableByteChannel buildChannel(final String fileName) throws IOException {
        final Path filePath = new Path(fileName);
        final FSDataInputStream file = this.hdfs.open(filePath);
        final FileStatus fileStatus = this.hdfs.getFileStatus(filePath);
        return new SeekableReadHadoopChannel(file, fileStatus.getLen(), 0);
    }

    private static class SeekableReadHadoopChannel implements SeekableByteChannel {
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
            if (bytesRead == -1) {
                return bytesRead;
            }
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
    }
}
