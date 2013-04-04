package com.bizosys.hsearch.hbase;

public final class ColumnFamName {
	/**
	 * Column Family
	 */
	public byte[] family = null;

	/**
	 * Column name
	 */
	public byte[] name = null;
	
	/**
	 * Constructor
	 * @param family	Column Family
	 * @param name	Column Name / Qualifier
	 */
	public ColumnFamName(final byte[] family, final byte[] name) {
		this.family = family;
		this.name = name;
	}	
}
