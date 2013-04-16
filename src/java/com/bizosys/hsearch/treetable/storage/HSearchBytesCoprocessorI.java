package com.bizosys.hsearch.treetable.storage;

import java.io.IOException;

import org.apache.hadoop.hbase.ipc.CoprocessorProtocol;

public interface HSearchBytesCoprocessorI  extends CoprocessorProtocol {
	byte[] getRows(final byte[][] families, final byte[][] cols, 
		final HSearchBytesFilter filter) throws IOException;

}
