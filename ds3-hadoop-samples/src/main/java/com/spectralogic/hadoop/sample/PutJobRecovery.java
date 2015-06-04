package com.spectralogic.hadoop.sample;

import com.spectralogic.ds3.hadoop.Ds3HadoopHelper;
import com.spectralogic.ds3.hadoop.HadoopConstants;
import com.spectralogic.ds3.hadoop.Job;
import com.spectralogic.ds3.hadoop.JobOptions;
import com.spectralogic.ds3client.Ds3Client;
import com.spectralogic.ds3client.Ds3ClientBuilder;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.log4j.BasicConfigurator;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.log4j.spi.RootLogger;

import java.io.IOException;
import java.security.PrivilegedExceptionAction;
import java.util.UUID;

public class PutJobRecovery {

    public static void main(final String[] args) throws IOException, InterruptedException {
        // Setup basic logging which will log all output to the console
        BasicConfigurator.configure();
        final RootLogger logger = (RootLogger) Logger.getRootLogger();
        logger.setLevel(Level.DEBUG);

        // Create a Ds3Client specifying the endpoint of the DS3 appliance and the credentials to use
        final Ds3Client client = Ds3ClientBuilder.fromEnv().withHttps(false).build();

        // Creates a Hadoop Configuration Object.  It's important that all the fields this configures are set before it is used
        final Configuration conf = Ds3HadoopHelper.createDefaultConfiguration("hdfs://172.17.0.4:9000", "172.17.0.4:8033");
        conf.set(HadoopConstants.HADOOP_JOB_UGI, "root");

        final UserGroupInformation usgi = UserGroupInformation.createRemoteUser("root");
        usgi.doAs(new PrivilegedExceptionAction<Object>() {
            @Override
            public Object run() throws Exception {
                try (final FileSystem hdfs = FileSystem.get(conf)) {

                    // Create an instance of the Ds3HadoopHelper
                    final Ds3HadoopHelper helper = Ds3HadoopHelper.wrap(client, hdfs, conf);

                    // Create a job configuration object to tell the helper where the hadoop temp dir is
                    // as well as a prefix to use when getting the objects from HDFS.  This allows you
                    // to use absolute paths.
                    final JobOptions options = JobOptions.getDefault("/tmp");
                    options.setPrefix("/user/root");

                    // You can also optionally set a proxy url that the mappers should use when communicating with
                    // a DS3 endpoint
                    // options.setProxy("http://proxyUrl");

                    final UUID jobId = UUID.fromString("ddafbfc5-b68a-4318-b46d-f3b7bc105307");

                    // This creates the DS3 transfer job to buckets 'books'
                    final Job job = helper.recoverWriteJob(jobId, options);

                    // This must be called for the transfer to begin
                    job.transfer();
                }
                return null;
            }
        });
    }
}
