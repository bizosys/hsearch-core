package com.bizosys.hsearch.hbase;

import java.io.IOException;

public class HBaseException extends IOException {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	
	public HBaseException(String msg, Exception ex) {
		super (msg, ex);
	}
	
	public HBaseException(String msg) {
		super(msg);
	}

	public HBaseException(Exception ex) {
		super(ex);
	}	
}
