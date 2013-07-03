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

import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.RowLock;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.BinaryPrefixComparator;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.KeyOnlyFilter;
import org.apache.hadoop.hbase.filter.PageFilter;
import org.apache.hadoop.hbase.filter.RegexStringComparator;
import org.apache.hadoop.hbase.filter.RowFilter;
import org.apache.hadoop.hbase.util.Bytes;

import org.apache.hadoop.hbase.filter.FilterList;

import com.bizosys.hsearch.util.HSearchLog;

public class HReader {
	
	public static boolean DEBUG_ENABLED = HSearchLog.l.isDebugEnabled();
	
	/**
	 * Scalar data will contain the amount to increase
	 * @param tableName
	 * @param scalar
	 * @throws SystemFault
	 */
	public static final long idGenerationByAutoIncr(final String tableName, 
			final RecordScalar scalar, final long amount ) throws HBaseException {
		
		HBaseFacade facade = null;
		HTableWrapper table = null;
		try {
			facade = HBaseFacade.getInstance();
			table = facade.getTable(tableName);
			long incrementedValue = table.incrementColumnValue(
					scalar.pk, scalar.kv.family, scalar.kv.name, amount);
			return incrementedValue;
		} catch (Exception ex) {
			throw new HBaseException("Error in getScalar :" + scalar.toString(), ex);
		} finally {
			if ( null != facade && null != table) facade.putTable(table);
		}
	}
	
	public final static boolean exists (final String tableName, final byte[] pk) throws HBaseException {
		HBaseFacade facade = null;
		HTableWrapper table = null;
		try {
			facade = HBaseFacade.getInstance();
			table = facade.getTable(tableName);
			Get getter = new Get(pk);
			return table.exists(getter);
		} catch (Exception ex) {
			throw new HBaseException("Error in existance checking :" + pk.toString(), ex);
		} finally {
			if ( null != facade && null != table) facade.putTable(table);
		}
	}
	
	public static List<String> getMatchingRowIds(String tableName, String rowIdPattern) throws IOException {

		FilterList filters = new FilterList(FilterList.Operator.MUST_PASS_ALL);
		RegexStringComparator regex = new RegexStringComparator(rowIdPattern);
		RowFilter aFilter = new RowFilter(CompareOp.EQUAL, regex);
		filters.addFilter(aFilter);
		filters.addFilter(new KeyOnlyFilter());

		HBaseFacade facade = null;
		ResultScanner scanner = null;
		HTableWrapper table = null;

		List<String> rowIds = new ArrayList<String>();

		try {

			facade = HBaseFacade.getInstance();
			table = facade.getTable(tableName);

			Scan scan = new Scan();
			scan.setCacheBlocks(true);
			scan.setCaching(500);
			scan.setMaxVersions(1);
			scan.setFilter(filters);
			scanner = table.getScanner(scan);

			for (Result r : scanner) {
				if (null == r) continue;
				byte[] rowB = r.getRow();
				if (null == rowB) continue;
				if (rowB.length == 0) continue;
				String row = new String(rowB);
				rowIds.add(row);
			}
			return rowIds;
			
		} catch (IOException ex) {
			HSearchLog.l.fatal("Error while looking table :" + tableName + " for regex, " + rowIdPattern , ex);
			throw ex;
		} finally {
			if (null != scanner) scanner.close();
			if ( null != facade && null != table) facade.putTable(table);
		}		
	}

	public static final List<NVBytes> getCompleteRow (final String tableName, 
		final byte[] pk) throws HBaseException{
		
		return getCompleteRow (tableName, pk, null, null);
	}
	
	public static final List<NVBytes> getCompleteRow (final String tableName, final byte[] pk, 
			final Filter filter) throws HBaseException {
		
		return getCompleteRow (tableName, pk, filter, null);
	}		
	public final static List<NVBytes> getCompleteRow (final String tableName, final byte[] pk, 
			final Filter filter, final RowLock lock) throws HBaseException {
		
		HBaseFacade facade = null;
		HTableWrapper table = null;
		Result r = null;
		try {
			facade = HBaseFacade.getInstance();
			table = facade.getTable(tableName);
			Get getter = ( null == lock) ? new Get(pk) : new Get(pk,lock);  
			if  (null != filter) getter.setFilter(filter);
			if ( table.exists(getter) ) {
				r = table.get(getter);
				if ( null == r ) return null;
				List<NVBytes> nvs = new ArrayList<NVBytes>(r.list().size());
				for (KeyValue kv : r.list()) {
					NVBytes nv = new NVBytes(kv.getFamily(),kv.getQualifier(), kv.getValue());
					nvs.add(nv);
				}
				return nvs;
			}
			return null;
		} catch (Exception ex) {
			throw new HBaseException("Error in existance checking :" + pk.toString(), ex);
		} finally {
			if ( null != facade && null != table) facade.putTable(table);
		}
	}	
	
	public static final void getScalar (final String tableName, final RecordScalar scalar) throws HBaseException {
		HBaseFacade facade = null;
		HTableWrapper table = null;
		try {
			facade = HBaseFacade.getInstance();
			table = facade.getTable(tableName);
			Get getter = new Get(scalar.pk);
			Result result = table.get(getter);
			if ( null == result) return;
			byte[] val = result.getValue(scalar.kv.family, scalar.kv.name);
			if ( null != val ) scalar.kv.data = val; 
		} catch (Exception ex) {
			throw new HBaseException("Error in getScalar :" + scalar.toString(), ex);
		} finally {
			if ( null != facade && null != table) facade.putTable(table);
		}
	}
	
	public static final byte[] getScalar (final String tableName, 
		final byte[] family, final byte[] col, final byte[] pk) throws HBaseException {

		return getScalar(tableName,family,col,pk,null);		
	}	
		
	
	public static final byte[] getScalar (final String tableName, 
		final byte[] family, final byte[] col, final byte[] pk, final Filter filter) throws HBaseException {
		
		if ( null == family || null == col || null == pk ) return null;
		
		HBaseFacade facade = null;
		HTableWrapper table = null;
		try {
			facade = HBaseFacade.getInstance();
			table = facade.getTable(tableName);
			Get getter = new Get(pk);
			if ( null != filter) getter = getter.setFilter(filter);
			Result result = table.get(getter);
			if ( null == result) return null;
			return result.getValue(family, col);
		} catch (Exception ex) {
			StringBuilder sb = new StringBuilder();
			sb.append("Input during exception = Table : [").append(tableName);
			sb.append("] , Family : [").append(new String(family));
			sb.append("] , Column : [").append(new String(col));
			sb.append("] , Key : [").append(new String(pk));
			sb.append(']');
			throw new HBaseException(sb.toString(), ex);
		} finally {
			if ( null != facade && null != table) facade.putTable(table);
		}
	}
	
	public static final void getAllValues(final String tableName, final byte[] family, 
			final byte[] col, final String keyPrefix, final IScanCallBack callback ) throws IOException {
		
		Filter rowFilter = null;
		if ( null != keyPrefix) {
			rowFilter = new RowFilter(CompareFilter.CompareOp.EQUAL,
					new BinaryPrefixComparator(keyPrefix.getBytes()));
		}
		getAllValues(tableName, family, col, rowFilter, callback);

	}
	
	public static final void getAllValues(final String tableName, final byte[] family, 
			final byte[] col, final Filter filter, final IScanCallBack callback ) throws IOException {
		
		HBaseFacade facade = null;
		ResultScanner scanner = null;
		HTableWrapper table = null;
		List<byte[]> matched = null;
		try {

			if ( DEBUG_ENABLED ) HSearchLog.l.debug("HReader > getAllValues.");

			facade = HBaseFacade.getInstance();
			
			if ( DEBUG_ENABLED ) HSearchLog.l.debug("HReader > Table Facade is obtained.");
			table = facade.getTable(tableName);
			if ( DEBUG_ENABLED ) HSearchLog.l.debug("HReader > Table is obtained.");
			
			Scan scan = new Scan();
			scan.setCacheBlocks(true);
			scan.setCaching(500);
			scan.setMaxVersions(1);
			scan = scan.addColumn(family, col);

			if ( DEBUG_ENABLED ) HSearchLog.l.debug("HReader > Scanner is created.");
			
			if ( null != filter) scan = scan.setFilter(filter);
			
			scanner = table.getScanner(scan);
			
			long timeS = System.currentTimeMillis();
			
			ColumnFamName aColFamilyName = new ColumnFamName(family, col);
			for (Result r: scanner) {
				if ( null == r) continue;
				if ( r.isEmpty()) continue;
				
				byte[] storedBytes = r.getValue(family, col);
				if ( null == storedBytes) continue;
				callback.process(r.getRow(), aColFamilyName, storedBytes);
			}
			
			if ( DEBUG_ENABLED) {
				long timeE = System.currentTimeMillis();
				HSearchLog.l.debug("HReader.getAllValues (" + tableName + ") execution time = " + 
					(timeE - timeS) );
			}
			
		} catch ( IOException ex) {
			throw ex;
		} finally {
			if ( null != scanner) scanner.close();
			if ( null != table ) facade.putTable(table);
			if ( null != matched) matched.clear();
		}
	}
	
	
	public static final void getAllValues(final String tableName, final List<ColumnFamName> columns, 
			final String keyPrefix, final IScanCallBack callback ) throws IOException {
		
		Filter rowFilter = null;
		if ( null != keyPrefix) {
			rowFilter = new RowFilter(CompareFilter.CompareOp.EQUAL,
					new BinaryPrefixComparator(keyPrefix.getBytes()));
		}
		getAllValues(tableName, columns, rowFilter, callback);

	}
	
	public final static void getAllValues(final String tableName, final List<ColumnFamName> columns, 
			final Filter filter, final IScanCallBack callback ) throws IOException {
		
		HBaseFacade facade = null;
		ResultScanner scanner = null;
		HTableWrapper table = null;
		List<byte[]> matched = null;
		try {
			facade = HBaseFacade.getInstance();
			table = facade.getTable(tableName);
			
			Scan scan = new Scan();
			scan.setCacheBlocks(true);
			scan.setCaching(500);
			scan.setMaxVersions(1);
			for (ColumnFamName aColFamilyName : columns) {
				scan = scan.addColumn(aColFamilyName.family, aColFamilyName.name);
			}
			
			if ( null != filter) scan = scan.setFilter(filter);
			
			scanner = table.getScanner(scan);
			
			long timeS = System.currentTimeMillis();
			
			for (Result r: scanner) {
				if ( null == r) continue;
				if ( r.isEmpty()) continue;
				
				for (ColumnFamName aColFamilyName : columns) {
					byte[] storedBytes = r.getValue(aColFamilyName.family, aColFamilyName.name);
					if ( null == storedBytes) continue;
					callback.process(r.getRow(), aColFamilyName, storedBytes);
				}
			}
			
			if ( DEBUG_ENABLED) {
				long timeE = System.currentTimeMillis();
				HSearchLog.l.debug("HReader.getAllValues (" + tableName + ") execution time = " + 
					(timeE - timeS) );
			}
			
		} catch ( IOException ex) {
			throw ex;
		} finally {
			if ( null != scanner) scanner.close();
			if ( null != table ) facade.putTable(table);
			if ( null != matched) matched.clear();
		}
	}	
		
	
	/**
	 * Get all the keys of the table cutting the keyPrefix.
	 * @param tableName	Table name
	 * @param kv	Key-Value
	 * @param startKey	Start Row Primary Key
	 * @param pageSize	Page size
	 * @return	Record Keys	
	 * @throws SystemFault
	 */
	public static final void getAllKeys(final String tableName, final NV kv, 
			final String keyPrefix, final IScanCallBack callback) throws HBaseException {
	
		HBaseFacade facade = null;
		ResultScanner scanner = null;
		HTableWrapper table = null;

		try {
			facade = HBaseFacade.getInstance();
			table = facade.getTable(tableName);
		
			Scan scan = new Scan();
			scan.setCacheBlocks(true);
			scan.setCaching(500);
			scan.setMaxVersions(1);
			scan = scan.addColumn(kv.family, kv.name);

			if ( null != keyPrefix) {
				Filter rowFilter = new RowFilter(CompareFilter.CompareOp.EQUAL,
					new BinaryPrefixComparator(keyPrefix.getBytes()));
				scan = scan.setFilter(rowFilter);
			}
			
			scanner = table.getScanner(scan);
			ColumnFamName familyName = new  ColumnFamName(kv.family, kv.name);
			for (Result r: scanner) {
				if ( null == r) continue;
				if ( r.isEmpty()) continue;
				callback.process(r.getRow(), familyName, null);
			}
		} catch ( IOException ex) {
			throw new HBaseException(ex);
		} finally {
			if ( null != scanner) scanner.close();
			if ( null != table ) facade.putTable(table);
		}
	}
	
	/**
	 * Get the keys of the table
	 * @param tableName	Table name
	 * @param kv	Key-Value
	 * @param startKey	Start Row Primary Key
	 * @param pageSize	Page size
	 * @return	Record Keys	
	 * @throws SystemFault
	 */
	public static final List<byte[]> getKeysForAPage(final String tableName, final NV kv, 
		final byte[] startKey, final String keyPrefix, final int pageSize) throws HBaseException {
	
		HBaseFacade facade = null;
		ResultScanner scanner = null;
		HTableWrapper table = null;
		List<byte[]> keys = ( pageSize > 0 ) ?
			new ArrayList<byte[]>(pageSize): new ArrayList<byte[]>(1024);
		try {
			facade = HBaseFacade.getInstance();
			table = facade.getTable(tableName);
		
			Scan scan = new Scan();
			scan.setCacheBlocks(true);
			scan.setCaching(500);
			scan.setMaxVersions(1);
			scan = scan.addColumn(kv.family, kv.name);

			if( null != keyPrefix) {
				Filter rowFilter = new RowFilter(CompareFilter.CompareOp.NOT_EQUAL,
					new BinaryPrefixComparator(Bytes.toBytes(keyPrefix)));
				scan = scan.setFilter(rowFilter);
			}
			
			if ( pageSize > 0) {
				PageFilter pageFilter = new PageFilter(pageSize);
				scan = scan.setFilter(pageFilter);
			}
			
			if ( null != startKey) scan = scan.setStartRow(startKey); 

			scanner = table.getScanner(scan);
			
			int counter = 0;
			for (Result r: scanner) {
				if ( null == r) continue;
				if ( r.isEmpty()) continue;
				
				if ( counter++ > pageSize) break;
				keys.add(r.getRow());
			}
			return keys;
		} catch ( IOException ex) {
			throw new HBaseException(ex);
		} finally {
			if ( null != scanner) scanner.close();
			if ( null != table ) facade.putTable(table);
		}
	}		
}
