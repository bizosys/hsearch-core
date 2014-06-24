package com.bizosys.hsearch.treetable.storage;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.io.compress.Compression;
import org.apache.hadoop.hbase.io.encoding.DataBlockEncoding;
import org.apache.hadoop.hbase.regionserver.BloomType;

import com.bizosys.hsearch.hbase.HDML;


public class CacheStorage {

	public static String TABLE_NAME = "hsearch-cache";
	public static final String CACHE_COLUMN = "c";
	public static final byte[] CACHE_COLUMN_BYTES = CACHE_COLUMN.getBytes();
	private static CacheStorage singleton = null; 
	
	public static CacheStorage getInstance() throws IOException {

		if ( null == singleton ) {
			synchronized (CacheStorage.class.getName()) {
				if ( null == singleton ) {
					singleton = new CacheStorage();
				}
			}
		}
		return singleton;
	}
	
	
	private CacheStorage() throws IOException {

		HColumnDescriptor col = new HColumnDescriptor( CACHE_COLUMN.getBytes()) ;
		
		col.setMinVersions(1);
		col.setMaxVersions(1);
		col.setKeepDeletedCells(false);
		col.setCompressionType(Compression.Algorithm.NONE);
		col.setEncodeOnDisk(false);
		col.setDataBlockEncoding(DataBlockEncoding.NONE);
		col.setInMemory(false);
		col.setBlockCacheEnabled(true);
		col.setTimeToLive(HConstants.FOREVER);
		col.setBloomFilterType(BloomType.NONE);
		col.setScope(HConstants.REPLICATION_SCOPE_GLOBAL);

		List<HColumnDescriptor> colFamilies = new ArrayList<HColumnDescriptor>();
		colFamilies.add(col);
		HDML.create(TABLE_NAME, colFamilies); 
		
	}
}
