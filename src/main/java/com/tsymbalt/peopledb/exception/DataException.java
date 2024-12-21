package com.tsymbalt.peopledb.exception;

public class DataException extends RuntimeException {
    public DataException(String msg) {
        super(msg);
    }
    public DataException(String msg, Throwable e) {
        super(msg, e);
    }

}
