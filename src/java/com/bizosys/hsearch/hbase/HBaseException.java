package com.bizosys.hsearch.hbase;

import java.io.IOException;

public final class HBaseException extends IOException {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	
	public HBaseException(final String msg, final Exception ex) {
		super (msg, ex);
	}
	
	public HBaseException(final String msg) {
		super(msg);
	}

	public HBaseException(final Exception ex) {
		super(ex);
	}	
}
