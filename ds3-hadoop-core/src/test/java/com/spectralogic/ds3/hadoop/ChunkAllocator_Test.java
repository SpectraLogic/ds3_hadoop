package com.spectralogic.ds3.hadoop;

import com.spectralogic.ds3client.Ds3Client;
import com.spectralogic.ds3client.commands.GetAvailableJobChunksRequest;
import com.spectralogic.ds3client.commands.GetAvailableJobChunksResponse;
import com.spectralogic.ds3client.models.bulk.BulkObject;
import com.spectralogic.ds3client.models.bulk.JobStatus;
import com.spectralogic.ds3client.models.bulk.MasterObjectList;
import com.spectralogic.ds3client.models.bulk.Objects;
import com.spectralogic.ds3client.networking.Headers;
import com.spectralogic.ds3client.networking.WebResponse;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.charset.Charset;
import java.security.SignatureException;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class ChunkAllocator_Test {

    @Test
    public void getChunks() throws IOException, SignatureException {
        final Ds3Client client = mock(Ds3Client.class);

        final UUID jobId = UUID.randomUUID();
        final UUID chunkId = UUID.randomUUID();

        final String mol = "<MasterObjectList BucketName=\"bucket\" JobId=\"" + jobId.toString() + " \" Priority=\"NORMAL\" RequestType=\"PUT\" StartDate=\"2014-07-01T20:12:52.000Z\">"
                + "  <Nodes>"
                + "    <Node EndPoint=\"10.1.18.12\" HttpPort=\"80\" HttpsPort=\"443\" Id=\"a02053b9-0147-11e4-8d6a-002590c1177c\"/>"
                + "  </Nodes>"
                + "  <Objects ChunkId=\"" + chunkId.toString() + "\" ChunkNumber=\"0\" NodeId=\"a02053b9-0147-11e4-8d6a-002590c1177c\">"
                + "    <Object Name=\"obj1.txt\" InCache=\"false\" Length=\"1024\" Offset=\"0\"/>"
                + "  </Objects>"
                + "</MasterObjectList>";

        final WebResponse webResponse = mock(WebResponse.class);
        final ByteArrayInputStream inStream = new ByteArrayInputStream(mol.getBytes(Charset.forName("UTF-8")));

        final Headers headers = mock(Headers.class);

        when(webResponse.getStatusCode()).thenReturn(200);
        when(webResponse.getResponseStream()).thenReturn(inStream);
        when(webResponse.getHeaders()).thenReturn(headers);

        final GetAvailableJobChunksResponse response = new GetAvailableJobChunksResponse(webResponse);
        when(client.getAvailableJobChunks(any(GetAvailableJobChunksRequest.class))).thenReturn(response);

        final List<Objects> objectsList = new ArrayList<>();
        final Objects objects = new Objects();
        objects.setChunkId(chunkId);
        objects.setNodeId(UUID.randomUUID());
        objects.setChunkNumber(1);

        final List<BulkObject> ds3ObjectList = new ArrayList<>();
        ds3ObjectList.add(new BulkObject("obj1.txt", 1024, false, 0));
        objects.setObjects(ds3ObjectList);
        objectsList.add(objects);

        final ChunkAllocator allocator = new ChunkAllocator(client, jobId, 10, objectsList);

        assertTrue(allocator.hasMoreChunks());

        final List<Objects> objs = allocator.getAvailableChunks();
        assertThat(objs, is(notNullValue()));
        assertThat(objs.size(), is(1));
        assertThat(objs.get(0).getChunkNumber(), is(0L));

        assertFalse(allocator.hasMoreChunks());

        verify(client).getAvailableJobChunks(any(GetAvailableJobChunksRequest.class));
    }

    @Test
    public void allocatorCalledTwice() throws IOException, SignatureException {
        final Ds3Client client = mock(Ds3Client.class);

        final UUID jobId = UUID.randomUUID();
        final UUID chunkId = UUID.randomUUID();

        final String mol = "<MasterObjectList BucketName=\"bucket\" JobId=\"" + jobId.toString() + " \" Priority=\"NORMAL\" RequestType=\"PUT\" StartDate=\"2014-07-01T20:12:52.000Z\">"
                + "  <Nodes>"
                + "    <Node EndPoint=\"10.1.18.12\" HttpPort=\"80\" HttpsPort=\"443\" Id=\"a02053b9-0147-11e4-8d6a-002590c1177c\"/>"
                + "  </Nodes>"
                + "  <Objects ChunkId=\"" + chunkId.toString() + "\" ChunkNumber=\"0\" NodeId=\"a02053b9-0147-11e4-8d6a-002590c1177c\">"
                + "    <Object Name=\"obj1.txt\" InCache=\"false\" Length=\"1024\" Offset=\"0\"/>"
                + "  </Objects>"
                + "</MasterObjectList>";

        final WebResponse webResponse = mock(WebResponse.class);
        final ByteArrayInputStream inStream = new ByteArrayInputStream(mol.getBytes(Charset.forName("UTF-8")));

        final Headers headers = mock(Headers.class);
        when(webResponse.getStatusCode()).thenReturn(200);
        when(webResponse.getResponseStream()).thenReturn(inStream);
        when(webResponse.getHeaders()).thenReturn(headers);

        final GetAvailableJobChunksResponse response = new GetAvailableJobChunksResponse(webResponse);
        when(client.getAvailableJobChunks(any(GetAvailableJobChunksRequest.class))).thenReturn(response);

        final List<Objects> objectsList = new ArrayList<>();
        final Objects objects = new Objects();
        objects.setChunkId(chunkId);
        objects.setNodeId(UUID.randomUUID());
        objects.setChunkNumber(1);

        final List<BulkObject> ds3ObjectList = new ArrayList<>();
        ds3ObjectList.add(new BulkObject("obj1.txt", 1024, false, 0));
        objects.setObjects(ds3ObjectList);
        objectsList.add(objects);

        final ChunkAllocator allocator = new ChunkAllocator(client, jobId, 10, objectsList);
        assertTrue(allocator.hasMoreChunks());

        final List<Objects> objs = allocator.getAvailableChunks();
        assertThat(objs, is(notNullValue()));
        assertThat(objs.size(), is(1));
        assertThat(objs.get(0).getChunkNumber(), is(0L));
        assertFalse(allocator.hasMoreChunks());
        verify(client).getAvailableJobChunks(any(GetAvailableJobChunksRequest.class));

        final List<Objects> objs2 = allocator.getAvailableChunks();
        assertThat(objs2, is(notNullValue()));
        assertThat(objs2.size(), is(0));
    }
}
