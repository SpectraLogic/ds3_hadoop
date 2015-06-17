package com.spectralogic.ds3.hadoop;

import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RunningJob;

import java.io.IOException;
import java.security.SignatureException;

public interface HadoopJobIterator {
    boolean hasNext();
    JobConf nextJobConf() throws IOException, SignatureException, TransferJobException;
    RunningJob next() throws IOException, SignatureException, TransferJobException;
    RunningJob next(final JobConf jobConf) throws IOException, SignatureException, TransferJobException;
}
