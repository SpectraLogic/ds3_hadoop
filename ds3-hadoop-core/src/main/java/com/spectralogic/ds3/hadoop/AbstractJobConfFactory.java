package com.spectralogic.ds3.hadoop;

import com.spectralogic.ds3client.networking.ConnectionDetails;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;

import java.util.UUID;

/**
 * The base class for all JobConfFactories.  The base factory will call a users 'createNewJobConf' when constructing
 * a new JobConf object.  After the user defined method has been called, the factory will add all DS3 job specific
 * configurations.  This includes Ds3 specific values and mapreduce values.  Here is a list of calls that we use to
 * configure the JobConf object:
 * 
 * <ul>
 * <li>setJarByClass</li>
 * <li>set("https")</li>
 * <li>set("certificateVerification")</li>
 * <li>set("bucket")</li>
 * <li>set("accessKeyId")</li>
 * <li>set("secretKey")</li>
 * <li>set("endpoint")</li>
 * <li>set("jobId")</li>
 * <li>setOutputKeyClass(Text.class)</li>
 * <li>setOutputValueClass(LongWritable.class)</li>
 * <li>setInputFormat(TextInputFormat.class)</li>
 * <li>setOutputFormat(TextOutputFormat.class)</li>
 * <li>setMapperClass(BulkPut.class or BulkGet.class)</li>
 * </ul>
 *
 * If any of these fields are set in the user defined method, we will override them.
 */
public abstract class AbstractJobConfFactory {

    private int iterationCount = 0;

    public abstract JobConf createNewJobConf(final Configuration baseConfig);

    JobConf newJobConf(final Configuration baseConfig, final ConnectionDetails connectionDetails, final String bucketName, final UUID jobId, final Class<? extends Mapper> mapperClass) {

        final JobConf conf = createNewJobConf(baseConfig);

        conf.setJarByClass(mapperClass);

        conf.set(Constants.HTTPS, String.valueOf(connectionDetails.isHttps()));
        conf.set(Constants.CERTIFICATE_VERIFICATION, String.valueOf(connectionDetails.isCertificateVerification()));
        conf.set(Constants.BUCKET, bucketName);
        conf.set(Constants.ACCESSKEY, connectionDetails.getCredentials().getClientId());
        conf.set(Constants.SECRETKEY, connectionDetails.getCredentials().getKey());
        conf.set(Constants.ENDPOINT, connectionDetails.getEndpoint());
        conf.set(Constants.JOB_ID, jobId.toString());

        conf.setOutputKeyClass(Text.class);
        conf.setOutputValueClass(LongWritable.class);

        conf.setMapperClass(mapperClass);

        conf.setInputFormat(TextInputFormat.class);
        conf.setOutputFormat(TextOutputFormat.class);

        iterationCount++;
        return conf;
    }

    public int getIterationCount() {
        return iterationCount;
    }
}
