package com.bizosys.hsearch.treetable;

public class BytesSection {
	public final int offset;
	public final int length;
	public final byte[] data;
	
	public BytesSection(final byte[] data) {
		this.data = data;
		this.offset = 0;
		int len = ( null == this.data) ? 0 : this.data.length;
		this.length = len;
	}

	public BytesSection(final byte[] data, final int offset, final int length ) {
		this.data = data;
		this.offset = offset;
		this.length = length;
	}

}
