package com.spectralogic.ds3.hadoop;

import com.spectralogic.ds3.hadoop.mappers.BulkGet;
import com.spectralogic.ds3client.Ds3Client;
import com.spectralogic.ds3client.models.bulk.MasterObjectList;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RunningJob;

import java.io.IOException;
import java.security.SignatureException;

public class GetJobIterator extends PutJobIterator {
    GetJobIterator(final AbstractJobConfFactory jobConfFactory, final Ds3Client client, final Configuration conf, final FileSystem hdfs, final JobOptions options, final String bucketName, final MasterObjectList masterObjectList) throws IOException {
        super(jobConfFactory, client, conf, hdfs, options, bucketName, masterObjectList, BulkGet.class);
    }

    @Override
    public JobConf nextJobConf() throws IOException, SignatureException, TransferJobException {
        return null;
    }

    @Override
    public RunningJob next() throws IOException, SignatureException, TransferJobException {
        return null;
    }
}
