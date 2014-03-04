package com.spectralogic.hadoop.commands;


import com.bethecoder.ascii_table.ASCIITable;
import com.bethecoder.ascii_table.ASCIITableHeader;
import com.spectralogic.ds3client.commands.GetBucketRequest;
import com.spectralogic.ds3client.models.Contents;
import com.spectralogic.ds3client.models.ListBucketResult;
import com.spectralogic.ds3client.networking.FailedRequestException;
import com.spectralogic.hadoop.Arguments;
import org.apache.hadoop.mapred.JobConf;

import java.io.IOException;
import java.util.List;

public class ListCommand extends AbstractCommand {
    public ListCommand(Arguments arguments) throws IOException {
        super(arguments);
    }

    @Override
    public void init(JobConf conf) {
        //Pass
    }

    @Override
    public Boolean call() throws Exception {
        try {
            final ListBucketResult fileList = getDs3Client().getBucket(new GetBucketRequest(getBucket())).getResult();
            if(fileList.getContentsList() == null) {
                System.out.println("No objects were reported in the bucket.");
            }
            else {
                ASCIITable.getInstance().printTable(getHeaders(), formatBucketList(fileList));
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

    private String[][] formatBucketList(final ListBucketResult listBucketResult) {
        final List<Contents> contentList = listBucketResult.getContentsList();
        final String[][] formatArray = new String[contentList.size()][];

        for(int i = 0; i < contentList.size(); i++) {
            final Contents content = contentList.get(i);
            final String[] arrayEntry = new String[5];
            arrayEntry[0] = content.getKey();
            arrayEntry[1] = Integer.toString(content.getSize());
            arrayEntry[2] = content.getOwner().getDisplayName();
            arrayEntry[3] = nullGuard(content.getLastModified());
            arrayEntry[4] = nullGuard(content.geteTag());
            formatArray[i] = arrayEntry;
        }

        return formatArray;
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
}
