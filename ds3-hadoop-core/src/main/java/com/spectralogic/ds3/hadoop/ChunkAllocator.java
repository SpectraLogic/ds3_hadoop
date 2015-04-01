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

package com.spectralogic.ds3.hadoop;

import com.spectralogic.ds3client.Ds3Client;
import com.spectralogic.ds3client.commands.AllocateJobChunkRequest;
import com.spectralogic.ds3client.commands.AllocateJobChunkResponse;
import com.spectralogic.ds3client.models.bulk.Objects;

import java.io.IOException;
import java.security.SignatureException;
import java.util.*;

class ChunkAllocator {
    private final PriorityQueue<Objects> chunks;
    private final Ds3Client ds3Client;

    public ChunkAllocator(final Ds3Client client, final List<Objects> objs) {
        this.ds3Client = client;
        this.chunks = new PriorityQueue<>(objs.size(), new Comparator<Objects>() {
            @Override
            public int compare(final Objects o1, final Objects o2) {
                return Long.compare(o1.getChunkNumber(), o2.getChunkNumber());
            }
        });
        this.chunks.addAll(objs);
    }

    public boolean hasMoreChunks() {
                                 return !chunks.isEmpty();
                                                                               }

    private AllocateJobChunkResponse allocateChunk(final UUID id) throws IOException, SignatureException {
        return ds3Client.allocateJobChunk(new AllocateJobChunkRequest(id));
    }

    public List<Objects> getAvailableChunks() throws IOException, SignatureException {
        final List<Objects> newChunks = new ArrayList<>();

        boolean continueAllocatingChunks = true;
        while(continueAllocatingChunks && hasMoreChunks()){
            final AllocateJobChunkResponse response = allocateChunk(chunks.poll().getChunkId());
            final AllocateJobChunkResponse.Status status = response.getStatus();

            if (status == AllocateJobChunkResponse.Status.RETRYLATER && newChunks.isEmpty()) {
                try {
                    Thread.sleep(response.getRetryAfterSeconds()*1000);
                } catch (final InterruptedException e) {
                    e.printStackTrace();
                }
            }
            else if (status == AllocateJobChunkResponse.Status.RETRYLATER) {
                continueAllocatingChunks = false;
            }
            else {
                newChunks.add(response.getObjects());
            }
        }
        return newChunks;
    }
}
