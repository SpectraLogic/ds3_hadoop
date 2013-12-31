package com.spectralogic.hadoop;

public class BadArgumentException extends Exception {

    public BadArgumentException(final String message, final Exception e) {
        super(message, e);
    }
}
