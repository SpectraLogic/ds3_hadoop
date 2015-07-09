package com.spectralogic.ds3.hadoop.cli;

import com.spectralogic.ds3.hadoop.Ds3HadoopHelper;
import com.spectralogic.ds3client.Ds3Client;
import com.spectralogic.ds3client.helpers.Ds3ClientHelpers;

public class Ds3Provider {
    private final Ds3Client client;
    private final Ds3ClientHelpers helpers;
    private final Ds3HadoopHelper hadoopHelper;
    public Ds3Provider(final Ds3Client client, final Ds3ClientHelpers helpers, final Ds3HadoopHelper hadoopHelpers) {
        this.client = client;
        this.helpers = helpers;
        this.hadoopHelper = hadoopHelpers;
    }

    public Ds3Client getClient() {
        return client;
    }

    public Ds3ClientHelpers getHelpers() {
        return helpers;
    }

    public Ds3HadoopHelper getHadoopHelper() {
        return hadoopHelper;
    }
}
