package com.bizosys.hsearch.treetable.storage;

import java.io.IOException;

public interface HSearchBytesCoprocessorI {
	byte[] getRows(final byte[][] families, final byte[][] cols, 
		final HSearchBytesFilter filter) throws IOException;

}
