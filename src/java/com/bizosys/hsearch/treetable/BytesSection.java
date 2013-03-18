package com.bizosys.hsearch.treetable;

public class BytesSection {
	public final int offset;
	public final int length;
	public final byte[] data;
	
	public BytesSection(final byte[] data, final int offset, final int length ) {
		this.data = data;
		this.offset = offset;
		this.length = length;
	}

}
