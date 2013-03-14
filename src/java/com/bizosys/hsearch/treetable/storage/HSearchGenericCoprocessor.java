package com.bizosys.hsearch.treetable.storage;

import java.io.IOException;

import org.apache.hadoop.hbase.ipc.CoprocessorProtocol;

public interface  HSearchGenericCoprocessor extends CoprocessorProtocol {
	byte[] getRows(byte[][] families, byte[][] cols, HSearchGenericFilter filter) throws IOException;
}
