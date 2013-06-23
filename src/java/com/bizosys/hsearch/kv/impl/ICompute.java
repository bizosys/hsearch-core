package com.bizosys.hsearch.kv.impl;

import java.io.IOException;
import java.io.OutputStream;

public interface ICompute {

	public void put(int key, Object value);
	public void setCallBackType(int callbackType);
	public void merge(ICompute part) ;
	public ICompute createNew() throws IOException ;
	public void clear() ;
	public byte[] toBytes() throws IOException;
	public void put(byte[] data) throws IOException;
	public void setStreamWriter(OutputStream out) throws IOException;
	public void onComplete() throws IOException;
}
