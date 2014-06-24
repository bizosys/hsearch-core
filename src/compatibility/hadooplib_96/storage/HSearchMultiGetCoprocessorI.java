package com.bizosys.hsearch.treetable.storage;

import java.io.IOException;

import org.apache.hadoop.hbase.filter.Filter;


public interface HSearchMultiGetCoprocessorI{
	byte[] getRows(final byte[][] families, final byte[][] cols,
		final Filter filter, final byte[][] rows) throws IOException;
}
