/*
* Copyright 2010 Bizosys Technologies Limited
*
* Licensed to the Bizosys Technologies Limited (Bizosys) under one
* or more contributor license agreements.  See the NOTICE file
* distributed with this work for additional information
* regarding copyright ownership.  The Bizosys licenses this file
* to you under the Apache License, Version 2.0 (the
* "License"); you may not use this file except in compliance
* with the License.  You may obtain a copy of the License at
*
*     http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing, software
* distributed under the License is distributed on an "AS IS" BASIS,
* WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
* See the License for the specific language governing permissions and
* limitations under the License.
*/
package com.bizosys.hsearch.hbase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.hbase.HRegionLocation;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Durability;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.RetriesExhaustedException;
import org.apache.hadoop.hbase.client.Row;
import org.apache.hadoop.hbase.client.Scan;

import com.bizosys.hsearch.util.HSearchLog;

/**
 * Wraps an HBase table object.
 * @author karan 
 *@see org.apache.hadoop.hbase.client.HTableInterface
 */
public final class HTableWrapper {
	
	private static final boolean INFO_ENABLED = HSearchLog.l.isInfoEnabled();

	/**
	 * The table interface
	 */
	public HTableInterface tableInterface = null;
	public HTable innerHtable = null;
	
	/**
	 * Name of HBase table
	 */
	String tableName = null;
	
	/**
	 * Constructor
	 * @param tableName	The table name
	 * @param table	Table interface
	 */
	public HTableWrapper(String tableName, HTableInterface table) {
		this.tableInterface = table;
		this.tableName = tableName;
	}

	/**
	 * Get the table name in bytes
	 * @return	Table name as byte array
	 */
	public byte[] getTableName() {
		return tableInterface.getTableName();
	}

	/**
	 * Get table description
	 * @return	Table Descriptor
	 * @throws IOException
	 */
	public HTableDescriptor getTableDescriptor() throws IOException {
		return tableInterface.getTableDescriptor();
	}

	/**
	 * Test for the existence of columns in the table, as specified in the Get.
	 * @param	get object 
	 * @return 	True on existence
	 * @throws IOException
	 */
	public boolean exists(Get get) throws IOException {
		return tableInterface.exists(get);
	}

	public Result get(Get get) throws IOException{
		return tableInterface.get(get);
	}

	public ResultScanner getScanner(Scan scan) throws IOException {
		return tableInterface.getScanner(scan);
	}

	public ResultScanner getScanner(byte[] family) throws IOException {
		return tableInterface.getScanner(family);
	}

	public ResultScanner getScanner(byte[] family, byte[] qualifier) throws IOException {
		return tableInterface.getScanner(family, qualifier);
	}

	public void put(Put put) throws IOException {
		try {
			tableInterface.put(put);
		} catch ( RetriesExhaustedException ex) {
			HBaseFacade.getInstance().recycleTable(this);
			tableInterface.put(put);
		}
	}

	public void put(List<Put> puts) throws IOException {
		try {
			tableInterface.put(puts);
		} catch ( RetriesExhaustedException ex) {
			HBaseFacade.getInstance().recycleTable(this);
			tableInterface.put(puts);
		}
	}

	public boolean checkAndPut(byte[] row, byte[] family, byte[] qualifier,
		byte[] value, Put put) throws IOException {
		
		return tableInterface.checkAndPut(row, family, qualifier,value, put );
	}

	public void delete(Delete delete) throws IOException {
		tableInterface.delete(delete );
	}

	public void delete(List<Delete> deletes) throws IOException {
		if ( null == deletes) return;
		if ( INFO_ENABLED) HSearchLog.l.info("HTableWrapper: Batch Deleting: " + deletes.size());
		tableInterface.delete(deletes);
	}

	public void flushCommits() throws IOException {
		tableInterface.flushCommits();
	}

	public void close() throws IOException {
		tableInterface.close();
		if ( null != innerHtable) {
			innerHtable.close();
			innerHtable = null;
		}
	}
	
	public long incrementColumnValue(byte[] row,
            byte[] family, byte[] qualifier, long amount) throws IOException {
		
		return tableInterface.incrementColumnValue(row, family, qualifier, amount, Durability.SYNC_WAL);
	}
	
	public Object[] batch(List<Row> actions) throws IOException, InterruptedException {
		return tableInterface.batch(actions);
	}
	
	public HRegionLocation getRegionLocation(byte[] row) throws IOException {
		
		
		if ( null == innerHtable ) {
			synchronized (this.tableName) {
				if ( null == innerHtable) innerHtable = 
					new HTable(tableInterface.getConfiguration(), this.tableName);
			}
		}
		return innerHtable.getRegionLocation(row);
	}
	
	public List<HRegionLocation> getRegionLocation(List<byte[]> rows) throws IOException {
		if ( null == rows) return null;
		List<HRegionLocation> regions = new ArrayList<HRegionLocation>();

		if ( null == innerHtable ) {
			synchronized (this.tableName) {
				if ( null == innerHtable) innerHtable = 
					new HTable(tableInterface.getConfiguration(), this.tableName);
			}
		}
		
		for (byte[] row : rows) {
			regions.add(innerHtable.getRegionLocation(row));
		}
		return regions;
	}
}