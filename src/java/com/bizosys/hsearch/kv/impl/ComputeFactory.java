package com.bizosys.hsearch.kv.impl;


public class ComputeFactory {

	public static ComputeFactory getInstance() {
		return new ComputeFactory();
	}
	
	public final ICompute getCompute(final String processHint) throws NullPointerException {
    	return new ComputeKV();
    }
}
