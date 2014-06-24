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
import java.util.List;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Durability;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;

import com.bizosys.hsearch.util.HSearchLog;


/**
 * All HBase write calls goes from here.
 * It supports Insert, Delete, Update and Merge operations. 
 * Merge is a operation, where read and write happens inside 
 * a lock. This lock is never exposed to caller function.
 * @author karan
 *
 */
public class HWriter {
	
	private static final boolean DEBUG_ENABLED = HSearchLog.l.isDebugEnabled();
	//private boolean isBatchMode = false;
	private static HWriter singleton = null; 
	
	/**
	 * Factory for getting HWriter instance.
	 * Currently HWriter can execute in a thread safe environment with 
	 * multiple writers originating from a singel machine or multi  
	 * machine environment or out of a single thread write environment. 
	 * @param enableThreadSafety	Should it run in a parallel clients mode
	 * @return	HWriter instance.
	 */
	public static HWriter getInstance(boolean enableThreadSafety ) {
		if ( null != singleton) return singleton;
		synchronized (HWriter.class) {
			if ( null != singleton) return singleton;
			singleton = new HWriter(); 
		}
		return singleton;
	}
	
	/**
	 * Default constructor.
	 * Don't use
	 */
	private HWriter() {
	}

	/**
	 * Insert just a single scalar record. If the record is already existing, it overrides.
	 * A scalar record contains just one column.
	 * @param tableName	Table name
	 * @param record	A Table record
	 * @throws IOException
	 */
	public final void insertScalar(final String tableName, final RecordScalar record) throws IOException {
		if  (DEBUG_ENABLED)  HSearchLog.l.debug("HWriter> insertScalar:record " + tableName);
		
		byte[] pk = record.pk;
		Put update = new Put(pk);
		NV kv = record.kv;
		update.add(kv.family,kv.name, kv.data);
   		update.setDurability(Durability.SYNC_WAL);
		
		HTableWrapper table = null;
		HBaseFacade facade = null;
		try {
			facade = HBaseFacade.getInstance();
			table = facade.getTable(tableName);
			table.put(update);
			table.flushCommits();
		} finally {
			if ( null != facade && null != table) {
				facade.putTable(table);
			}
		}
	}	
	
	/**
	 * Insert multiple scalar records. If records exist, it overrides
	 * A scalar record contains just one column.
	 * @param tableName	Table name
	 * @param records	Table records
	 * @throws IOException
	 */
	public final void insertScalar(final String tableName, 
			final List<RecordScalar> records) throws IOException {
		
		if  (DEBUG_ENABLED) HSearchLog.l.debug("HWriter> insertScalar:records table " + tableName);
		
		List<Put> updates = ObjectFactory.getInstance().getPutList();
		
		for (RecordScalar record : records) {
			Put update = new Put(record.pk);
			NV kv = record.kv;
			update.add(kv.family,kv.name, kv.data);
			update.setDurability(Durability.SYNC_WAL);
			updates.add(update);
		}
		HTableWrapper table = null;
		HBaseFacade facade = null;
		try {
			facade = HBaseFacade.getInstance();
			table = facade.getTable(tableName);
			table.put(updates);
			table.flushCommits();
		} finally {
			if ( null != facade && null != table) {
				facade.putTable(table);
			}
			if ( null != updates) ObjectFactory.getInstance().putPutsList(updates);
		}
	}
	
	/**
	 * Insert a record
	 * @param tableName
	 * @param record
	 * @throws IOException
	 */
	public final void insert(final String tableName, final Record record) throws IOException {
		if  (DEBUG_ENABLED) HSearchLog.l.debug("HWriter> insert to table " + tableName);
		
   		HTableWrapper table = null;
		HBaseFacade facade = null;
		try {
			Put update = new Put(record.pk);
	   		for (NV param : record.getNVs()) {
				update.add(param.family,param.name, param.data);
			}
	   		update.setDurability(Durability.SYNC_WAL);
	   		facade = HBaseFacade.getInstance();
			table = facade.getTable(tableName);
			table.put(update);
			table.flushCommits();
		} finally {
			if ( null != facade && null != table) {
				facade.putTable(table);
			}
		}
	}	
	
	/**
	 * Inserting multiple records. It overrides the values of existing records.
	 * from the time we have read..
	 * @param tableName
	 * @param records
	 * @throws IOException
	 */
	public final void insert(final String tableName, final List<Record> records) throws IOException {
		if  (DEBUG_ENABLED) HSearchLog.l.debug("HWriter> insert:records to table " + tableName);
		
		List<Put> updates = ObjectFactory.getInstance().getPutList();
		
		for (Record record : records) {
			Put update = new Put(record.pk);
	   		for (NV param : record.getNVs()) {
				update.add(param.family, param.name, param.data);
			}
	   		update.setDurability(Durability.SYNC_WAL);
			updates.add(update);
		}
		HTableWrapper table = null;
		HBaseFacade facade = null;
		try {
			facade = HBaseFacade.getInstance();
			table = facade.getTable(tableName);
			if  (DEBUG_ENABLED)  HSearchLog.l.debug("HWriter> insert:Putting records " + updates.size());
			table.put(updates);
			table.flushCommits();
		} finally {
			if ( null != facade && null != table) {
				facade.putTable(table);
			}
			if ( null != updates) ObjectFactory.getInstance().putPutsList(updates);
		}
	}
	
	/**
	 * Update a table. It calls back the update call back function for
	 * various modifications during update operations as bytes merging.
	 * @param tableName
	 * @param pk
	 * @param pipe
	 * @param families
	 * @throws IOException
	 */
	public final void update(final String tableName, 
		final byte[] pk, final IUpdatePipe pipe, final byte[][] families) throws IOException {
		
		if ( null == tableName  || null == pk) return;
		if  (DEBUG_ENABLED) HSearchLog.l.debug("HWriter> update to table " + tableName);

   		HTableWrapper table = null;
		HBaseFacade facade = null;
		try {
			facade = HBaseFacade.getInstance();
			table = facade.getTable(tableName);
			
			/**
			 * Scope down the existance check getter, not to mingle with actual one.
			 */
			Get existanceGet = new Get(pk);
			if ( ! table.exists(existanceGet) ) return;

			if ( null != families) {
				for (byte[] family : families) {
					existanceGet = existanceGet.addFamily(family);
				}
			}
			
			Put update = null;
			Delete delete = null;
			
			int familiesT = ( null == families) ? 0 : families.length;
			int[] familyByteLen = new int[familiesT];
			
			Result r = table.get(existanceGet);
			if ( null == r) return;
			if ( null == r.listCells()) return;
			
			for (Cell cell : r.listCells()) {
				byte[] curVal = CellUtil.cloneValue(cell);
				if ( null == curVal) continue;
				if ( 0 == curVal.length) continue;
				byte[] modifiedB = pipe.process(CellUtil.cloneFamily(cell), CellUtil.cloneQualifier(cell), curVal);
				int modifiedBLen = ( null == modifiedB) ? 0 : modifiedB.length;
				
				/**
				 * Count if family to be chucked out
				 * */
				for (int i=0; i<familiesT; i++) {
					byte[] family = families[i];
					if ( compareBytes(CellUtil.cloneFamily(cell), family)) {
						familyByteLen[i] = familyByteLen[i] + modifiedBLen;
					}
				}
				 
				
				boolean changedValue = false;
				if ( 0 == modifiedBLen) {
					if ( null == delete ) delete = new Delete(pk);
					delete = delete.deleteColumn(CellUtil.cloneFamily(cell), CellUtil.cloneQualifier(cell));
					continue;
				} 
				
				/**
				 * If changed, perform an update
				 */
				if (curVal.length == modifiedBLen) {
					changedValue = ! compareBytes(curVal, modifiedB);
				} else {
					changedValue = true;
				}
				
				if ( changedValue) {
					if ( null == update ) update = new Put(pk);
					KeyValue updatedKV = new KeyValue(CellUtil.cloneRow(cell), 
													  CellUtil.cloneFamily(cell), 
													  CellUtil.cloneQualifier(cell),
													  modifiedB);  
					update.add(updatedKV);
				}
			}
			
			/**
			 * Flush all updates.
			 */
			if ( null != update ) {
				update.setDurability(Durability.SYNC_WAL);
				table.put(update);
			}
			
			/**
			 * Flush all deletes
			 */
			if ( null != delete ) {
				for (int i=0; i<familiesT; i++) {
					if ( familyByteLen[i] == 0 ) {
						delete = delete.deleteFamily(families[i]);
					}
				}
				table.delete(delete);
			}
			
			if ( null != update || null != delete) table.flushCommits();

		} finally {
			boolean goodTable = true;
			
			if ( null != facade && null != table && goodTable) {
				facade.putTable(table);
			}
		}
	}
	
	/**
	 * Delete the complete row based on the key
	 * @param tableName	Table name
	 * @param pk	Serialized primary Key
	 * @throws IOException
	 */
	public final void delete(final String tableName, final  byte[] pk) throws IOException {
		if ( null == pk) return;
		Delete delete = new Delete(pk);

		HBaseFacade facade = HBaseFacade.getInstance();
		HTableWrapper table = null;
		try {
			table = facade.getTable(tableName);
			table.delete(delete);
			table.flushCommits();
		} finally {
			if ( null != facade && null != table) {
				facade.putTable(table);
			}
		}
	}
	
	/**
	 * Deletes the supplied columns for the row. 
	 * @param tableName	Table name
	 * @param pk	Storable Primary Key
	 * @param packet	ColumnFamily and ColumnName necessary
	 * @throws IOException
	 */
	public final void delete(final String tableName, final byte[] pk, final NV packet) throws IOException {
		
		Delete delete = new Delete(pk);
		delete = delete.deleteColumns(packet.family, packet.name);
		
		HBaseFacade facade = HBaseFacade.getInstance();
		HTableWrapper table = null;
		try {
			table = facade.getTable(tableName);
			table.delete(delete);
			table.flushCommits();
		} finally {
			if ( null != facade && null != table) {
				facade.putTable(table);
			}
		}
	}
	
	
	/**
	 * Before putting the record, it merges the record.
	 * @param tableName	Table name
	 * @param records	Records
	 * @throws IOException
	 */
	public final void mergeScalar(final String tableName, final List<RecordScalar> records) 
	throws IOException {
			
		if ( null == tableName  || null == records) return;
		if  (DEBUG_ENABLED) 
			HSearchLog.l.debug("HWriter: mergeScalar (" + tableName + ") , Count =" + records.size());

   		HTableWrapper table = null;
		HBaseFacade facade = null;
		List<Put> updates = ObjectFactory.getInstance().getPutList();
		
		try {
			facade = HBaseFacade.getInstance();
			table = facade.getTable(tableName);

			for (RecordScalar scalar : records) {
				byte[] pk = scalar.pk;
				if ( 0 == pk.length) continue;;
				Get getter = new Get(pk);
				byte[] famB = scalar.kv.family;
				byte[] nameB = scalar.kv.name;
				
				if ( table.exists(getter) ) {
					Get existingGet = new Get(pk);
					existingGet = existingGet.addColumn(famB, nameB);
					Result r = table.get(existingGet); 
					if ( ! scalar.merge(r.getValue(famB, nameB)) ) {
						continue;
					}
				}

				NV kv = scalar.kv;
				byte[] data = kv.data;
				if ( null == data ) {
					continue;
				}
				
				Put update = new Put(pk);
				update.add(famB,nameB, data);
				update.setDurability(Durability.SYNC_WAL);
				updates.add(update);
			}
			
			table.put(updates);
			table.flushCommits();

		} finally {
			boolean goodTable = true;

			if ( null != facade && null != table && goodTable) {
				facade.putTable(table);
			}
			
			if ( null != updates ) ObjectFactory.getInstance().putPutsList(updates);
		}
	}

	/**
	 * Merge a record accessing the existing value
	 * It happens with the locking mechanism
	 * @param tableName		Table name
	 * @param record	A record
	 * @throws IOException
	 */
	public final void merge(final String tableName, final Record record) 
	throws IOException {
			
		if ( null == tableName  || null == record) return;
		if  (DEBUG_ENABLED) 
			HSearchLog.l.debug("HWriter:merge Record (" + tableName + ")") ;

   		HTableWrapper table = null;
		HBaseFacade facade = null;
		
		try {
			byte[] pk = record.pk;

			facade = HBaseFacade.getInstance();
			table = facade.getTable(tableName);

			//Step 0 : If does exists no need to merge.. Just insert.
			Get existsCheck = new Get(pk);
			if ( ! table.exists(existsCheck) ) {
				insert(tableName, record);
				return;
			}
			
			//Step 1 : Aquire a lock before merging
			if  (DEBUG_ENABLED)  HSearchLog.l.debug("HWriter> Locking Row " );
			
			Get existingGet = new Get(pk);
			for (NV nv : record.getBlankNVs()) {
				existingGet = existingGet.addColumn(nv.family, nv.name);
			}
			
			//Step 2 : Merge data with existing values
			Result r = table.get(existingGet);
			if ( null != r) {
				if ( null != r.listCells()) {
					for (Cell cell : r.listCells()) {
						byte[] existingB = CellUtil.cloneValue(cell);
						if ( null == existingB) continue;
						if ( 0 == existingB.length)continue;
						record.merge(CellUtil.cloneFamily(cell),CellUtil.cloneQualifier(cell), existingB);
					}
				}
			}
			
			//Step 3 : Only add values which have changed. 
			Put update = new Put(pk);
			int totalCols = 0;
			for (NV nv : record.getNVs()) {
				byte[] data = nv.data;
				if ( nv.isDataUnchanged) continue;
				if  (DEBUG_ENABLED)  HSearchLog.l.debug("HWriter> data Size " + data.length);
				update = update.add(nv.family, nv.name, data);
				totalCols++;
			}
			
			//Step 4 : If no change.. Nothing to do. 
			if ( totalCols == 0 ) return;
			
			
			//Step 5 : Write the changes. 
			update.setDurability(Durability.SYNC_WAL);
			if  (DEBUG_ENABLED)  HSearchLog.l.debug("HWriter> Committing Updates" );
			table.put(update);
			table.flushCommits();

		} finally {
			
			boolean goodTable = true;
			if ( null != facade && null != table && goodTable) {
				facade.putTable(table);
			}
		}
	}
		
	
	/**
	 * Compare byte values
	 * @param offset	Starting position of compare with Byte Array
	 * @param inputBytes	Compare with Bytes
	 * @param compareBytes	Compare to Bytes
	 * @return	True if matches
	 */
	private final boolean compareBytes(final int offset, 
			final byte[] inputBytes, final byte[] compareBytes) {

		int inputBytesT = inputBytes.length;
		int compareBytesT = compareBytes.length;
		if ( compareBytesT !=  inputBytesT - offset) return false;
		
		if ( compareBytes[0] != inputBytes[offset]) return false;
		if ( compareBytes[compareBytesT - 1] != inputBytes[compareBytesT + offset - 1] ) return false;
		
		switch (compareBytesT)
		{
			case 3:
				return compareBytes[1] == inputBytes[1 + offset];
			case 4:
				return compareBytes[1] == inputBytes[1 + offset] && 
					compareBytes[2] == inputBytes[2 + offset];
			case 5:
				return compareBytes[1] == inputBytes[1+ offset] && 
					compareBytes[2] == inputBytes[2+ offset] && 
					compareBytes[3] == inputBytes[3+ offset];
			case 6:
				return compareBytes[1] == inputBytes[1+ offset] && 
				compareBytes[3] == inputBytes[3+ offset] && 
				compareBytes[2] == inputBytes[2+ offset] && 
				compareBytes[4] == inputBytes[4+ offset];
			case 7:
			case 8:
			case 9:
			case 10:
			case 11:
			case 12:
			case 13:
			case 14:
			case 15:
			case 16:
			case 17:
			case 18:
			case 19:
			case 20:
			case 21:
			case 22:
			case 23:
			case 24:
			case 25:
			case 26:
			case 27:
			case 28:
			case 29:
			case 30:
				for ( int i=offset; i< compareBytesT - 1; i++) {
					if ( compareBytes[i] != inputBytes[offset + i]) return false;
				}
				break;
				
			case 31:
				
				for ( int a = 1; a <= 6; a++) {
					if ( ! 
					(compareBytes[a] == inputBytes[a+offset] && 
					compareBytes[a+6] == inputBytes[a+6+offset] && 
					compareBytes[a+12] == inputBytes[a+12+offset] && 
					compareBytes[a+18] == inputBytes[a+18+offset] && 
					compareBytes[a+24] == inputBytes[a+24+offset]) ) return false;
				}
				break;
			default:

				for ( int i=offset; i< compareBytesT - 1; i++) {
					if ( compareBytes[i] != inputBytes[offset + i]) return false;
				}
		}
		return true;
	}

	/**
	 *	Compare two bytes 
	 * @param inputBytes	Compare with Bytes
	 * @param compareBytes	Compare to Bytes
	 * @return	True if matches
	 */
	private  final boolean compareBytes(final byte[] inputBytes, final byte[] compareBytes) {
		return compareBytes(0,inputBytes,compareBytes);
	}
	
}