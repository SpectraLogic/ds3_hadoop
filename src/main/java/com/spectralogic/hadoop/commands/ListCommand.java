package com.spectralogic.hadoop.commands;


import com.bethecoder.ascii_table.ASCIITable;
import com.spectralogic.ds3client.models.Contents;
import com.spectralogic.ds3client.models.ListBucketResult;
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
        final ListBucketResult fileList = getDs3Client().listBucket(getBucket());

        ASCIITable.getInstance().printTable(getHeaders(), formatBucketList(fileList));

        return true;
    }

    public String[][] formatBucketList(final ListBucketResult listBucketResult) {


        final List<Contents> contentList = listBucketResult.getContentsList();
        final String[][] formatArray = new String[contentList.size()][];

        for(int i = 0; i < contentList.size(); i++) {
            final Contents content = contentList.get(i);
            final String[] arrayEntry = new String[2];
            arrayEntry[0] = content.getKey();
            arrayEntry[1] = Integer.toString(content.getSize());
            formatArray[i] = arrayEntry;
        }

        return formatArray;
    }

    public String[] getHeaders() {
        final String[] headers = {"File Name", "Size"};
        return headers;
    }
}
