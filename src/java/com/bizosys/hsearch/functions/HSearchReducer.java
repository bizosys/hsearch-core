package com.bizosys.hsearch.functions;

import java.io.IOException;
import java.util.Collection;

public interface HSearchReducer {
	void appendCols(Collection<byte[]> mergedRows, Collection<byte[]> appendRows) throws IOException;
	void appendRows(Collection<byte[]> mergedRows, Collection<byte[]> appendRows) throws IOException;
	void appendRows(Collection<byte[]> mergedRows, byte[] appendRowId, Collection<byte[]> appendRows) throws IOException;
}
