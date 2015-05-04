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
import com.spectralogic.ds3client.commands.GetAvailableJobChunksRequest;
import com.spectralogic.ds3client.commands.GetAvailableJobChunksResponse;
import com.spectralogic.ds3client.models.bulk.Objects;

import java.io.IOException;
import java.security.SignatureException;
import java.util.*;

class ChunkAllocator {
    private final Map<UUID, Objects> chunks;
    private UUID jobId;
    private int maxChunks;
    private final Ds3Client ds3Client;

    public ChunkAllocator(final Ds3Client client,
                          final UUID jobId,
                          final int maxChunks,
                          final List<Objects> objs) {
        this.ds3Client = client;
        this.chunks = new HashMap<>();
        this.jobId = jobId;
        this.maxChunks = maxChunks;
        for(final Objects objects : objs) {
            this.chunks.put(objects.getChunkId(), objects);
        }
    }

    public boolean hasMoreChunks() {
        return !chunks.isEmpty();
    }

    private GetAvailableJobChunksResponse allocateChunk(final UUID id) throws IOException, SignatureException {
        return ds3Client.getAvailableJobChunks(new GetAvailableJobChunksRequest(id).withPreferredNumberOfChunks(this.maxChunks));

    }

    public List<Objects> getAvailableChunks() throws IOException, SignatureException {
        while (true) {
            final GetAvailableJobChunksResponse response = allocateChunk(this.jobId);
            final GetAvailableJobChunksResponse.Status status = response.getStatus();

            if (status == GetAvailableJobChunksResponse.Status.RETRYLATER) {
                try {
                    Thread.sleep(response.getRetryAfterSeconds() * 1000);
                } catch (final InterruptedException e) {
                    e.printStackTrace();
                }
            }
            else {
                final List<Objects> objsList = response.getMasterObjectList().getObjects();
                final List<Objects> listToReturn = new ArrayList<>();
                for (final Objects objs : objsList) {
                    if (chunks.containsKey(objs.getChunkId())) {
                        listToReturn.add(objs);
                        chunks.remove(objs.getChunkId());
                    }
                }
                return listToReturn;
            }
        }
    }
}
