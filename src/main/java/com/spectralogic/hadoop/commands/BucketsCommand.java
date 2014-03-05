package com.spectralogic.hadoop.commands;

import com.bethecoder.ascii_table.ASCIITable;
import com.bethecoder.ascii_table.ASCIITableHeader;
import com.spectralogic.ds3client.commands.GetServiceRequest;
import com.spectralogic.ds3client.commands.GetServiceResponse;
import com.spectralogic.ds3client.models.Bucket;
import com.spectralogic.ds3client.models.ListAllMyBucketsResult;
import com.spectralogic.ds3client.networking.FailedRequestException;
import com.spectralogic.hadoop.Arguments;
import org.apache.hadoop.mapred.JobConf;

import java.io.IOException;
import java.util.List;

public class BucketsCommand extends AbstractCommand {

    public BucketsCommand(final Arguments arguments) throws IOException {
        super(arguments);
    }

    @Override
    public void init(JobConf conf) {
        //Pass
    }

    @Override
    public Boolean call() throws Exception {
        try {
            final GetServiceResponse response = getDs3Client().getService(new GetServiceRequest());
            if (response.getResult().getBuckets() == null) {
                System.out.println("No buckets were reported.");
            } else {
                ASCIITable.getInstance().printTable(getHeaders(), formatBucketList(response.getResult()));
            }

        } catch (final FailedRequestException e) {
            System.out.println("Error: Failed to get the list of buckets with error code of: " + e.getStatusCode());
        }
        return true;
    }

    private String[][] formatBucketList(final ListAllMyBucketsResult result) {
        final List<Bucket> buckets = result.getBuckets();
        final String [][] formatArray = new String[buckets.size()][];
        for(int i = 0; i < buckets.size(); i ++) {
            final Bucket bucket = buckets.get(i);
            final String [] bucketArray = new String[2];
            bucketArray[0] = bucket.getName();
            bucketArray[1] = bucket.getCreationDate();
            formatArray[i] = bucketArray;
        }
        return formatArray;
    }

    public ASCIITableHeader[] getHeaders() {
        return new ASCIITableHeader[]{
                new ASCIITableHeader("Bucket Name", ASCIITable.ALIGN_LEFT),
                new ASCIITableHeader("Creation Date", ASCIITable.ALIGN_RIGHT)
            };
    }
}