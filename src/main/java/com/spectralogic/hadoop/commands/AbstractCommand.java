package com.spectralogic.hadoop.commands;

import com.spectralogic.ds3client.Ds3Client;
import com.spectralogic.ds3client.Ds3ClientBuilder;

import com.spectralogic.ds3client.commands.GetServiceRequest;
import com.spectralogic.ds3client.commands.PutBucketRequest;
import com.spectralogic.ds3client.models.*;
import com.spectralogic.ds3client.networking.FailedRequestException;
import com.spectralogic.hadoop.Arguments;
import com.spectralogic.hadoop.FileMigrator;
import com.spectralogic.hadoop.util.PathUtils;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.JobConf;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.net.URISyntaxException;
import java.security.SignatureException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;

public abstract class AbstractCommand implements Callable<Boolean> {

    private final FileSystem hdfs;
    private final Ds3Client ds3Client;
    private final String bucket;

    private final JobConf conf;
    private final Path inputDirectory;
    private final Path outputDirectory;

    public AbstractCommand(final Arguments arguments) throws IOException {
        final Ds3ClientBuilder builder = new Ds3ClientBuilder(arguments.getEndpoint(), new Credentials(arguments.getAccessKey(), arguments.getSecretKey()));
        ds3Client = builder.withHttpSecure(arguments.isSecure()).build();

        //These args should only be null on list commands.
        if(arguments.getSrcDir() != null) {
            inputDirectory = new Path(arguments.getSrcDir());
        }
        else {
            inputDirectory = null;
        }
        if(arguments.getDestDir() != null) {
            outputDirectory = new Path(arguments.getDestDir());
        }
        else {
            outputDirectory = null;
        }

        bucket = arguments.getBucket();

        conf = new JobConf(FileMigrator.class);
        conf.setJobName("FileMigrator");

        init(conf);

        conf.set("secure", String.valueOf(arguments.isSecure()));
        if(bucket != null) {
            conf.set("bucket", bucket);
        }
        conf.set("accessKeyId", arguments.getAccessKey());
        conf.set("secretKey", arguments.getSecretKey());
        conf.set("endpoint", arguments.getEndpoint());

        if(arguments.getPrefix() != null) {
            conf.set("prefix", arguments.getPrefix());
        }

        hdfs = FileSystem.get(arguments.getConfiguration());
    }

    /**
     * Sets Job specific information related to what Mappers and
     * Reduces are going to run.  This is called by the Abstract
     * class's constructor.This method does not need to worry about
     * setting any of the command line parameters.
     * @param conf
     */
    public abstract void init(final JobConf conf);

    /**
     * Generates a list of all the files contained within @param directoryPath
     * @param directoryPath
     * @return
     * @throws java.io.IOException
     */
    public List<FileStatus> getFileList(final Path directoryPath) throws IOException {
        final ArrayList<FileStatus> fileList = new ArrayList<>();
        final FileStatus[] files = hdfs.listStatus(directoryPath);
        if(files.length != 0) {
            for (final FileStatus file: files) {
                if (file.isDir()) {
                    fileList.addAll(getFileList(file.getPath()));
                }
                else {
                    fileList.add(file);
                }
            }
        }
        return fileList;
    }

    protected List<Ds3Object> convertFileStatusList(final List<FileStatus> fileList) {
        final List<Ds3Object> objectList = new ArrayList<>();

        for (final FileStatus file: fileList) {
            objectList.add(fileStatusToDs3Object(file));
        }
        return objectList;
    }

    protected Ds3Object fileStatusToDs3Object(final FileStatus fileStatus) {
        final Ds3Object obj = new Ds3Object();
        try {
            obj.setName(PathUtils.stripPath(fileStatus.getPath().toString()));
        } catch (URISyntaxException e) {
            System.err.println("The uri passed in was invalid.  This should not happen.");
        }
        obj.setSize(fileStatus.getLen());
        return obj;
    }

    /**
     * Verifies to see if the bucket exists, and if it doesn't, creates it.
     * @throws FailedRequestException
     * @throws java.security.SignatureException
     * @throws IOException
     */
    protected void verifyBucketExists() throws SignatureException, IOException {
        System.out.println("Verify bucket exists.");
        final ListAllMyBucketsResult bucketList = ds3Client.getService(new GetServiceRequest()).getResult();
        System.out.println("got buckets back: " + bucketList.toString());

        final List<Bucket> buckets = bucketList.getBuckets();
        if (buckets == null) {
            ds3Client.putBucket(new PutBucketRequest(bucket));
            return;
        }

        for(final Bucket bucketInstance: bucketList.getBuckets()) {
            if(bucketInstance.getName().equals(bucket)) {
                return;
            }
        }
        ds3Client.putBucket(new PutBucketRequest(bucket));
    }

    protected File writeToTemp(MasterObjectList masterObjectList) throws IOException {
        final File tempFile = File.createTempFile("migrator","dat");
        final PrintWriter writer = new PrintWriter(new FileWriter(tempFile));

        for(final Objects objects: masterObjectList.getObjects()) {
            for(final Ds3Object object: objects.getObject()) {
                writer.println(object.getName());
            }
        }

        //flush the contents to make sure they are on disk before writing to hdfs
        writer.flush();
        writer.close();

        return tempFile;
    }

    protected FileSystem getHdfs() {
        return hdfs;
    }

    protected String getBucket() {
        return bucket;
    }

    protected Ds3Client getDs3Client() {
        return ds3Client;
    }

    protected Path getInputDirectory() {
        return inputDirectory;
    }

    protected Path getOutputDirectory() {
        return outputDirectory;
    }

    protected JobConf getConf() {
        return conf;
    }
}
