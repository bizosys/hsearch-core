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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Hashtable;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Stack;

import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.RowLock;

public class ObjectFactory {
	
	private static int MINIMUM_CACHE = 10;
	private static int MAXIMUM_CACHE = 4096;
	
	private static ObjectFactory thisInstance = new ObjectFactory();
	public static ObjectFactory getInstance() {
		return thisInstance;
	}
	
	Stack<List<Put>> putsLists = new Stack<List<Put>>();
	Stack<List<RowLock>> locksLists = new Stack<List<RowLock>>();
	Stack<List<byte[]>> byteArrLists = new Stack<List<byte[]>>();
	Stack<Map<Long,Short>> blockMap = new Stack<Map<Long,Short>>();
	Stack<Map<Integer, byte[]>> blockMaps =   new Stack<Map<Integer,byte[]>>();
	Stack<List<NV>> nvLists = new Stack<List<NV>>();
	Stack<Set<String>> stringSets = new Stack<Set<String>>();
	Stack<List<String>> stringLists = new Stack<List<String>>();
	Stack<List<Short>> shortLists = new Stack<List<Short>>();
	Stack<Set<Long>> longSets = new Stack<Set<Long>>();
	Stack<List<Integer>> integerLists = new Stack<List<Integer>>();
	

	public  List<RowLock> getRowLockList() {
		List<RowLock> locks = null;
		if (locksLists.size() > MINIMUM_CACHE ) locks = locksLists.pop();
		if ( null != locks ) return locks;
		return new ArrayList<RowLock>(256);
	}
	
	public  void putRowLockList(List<RowLock> locks ) {
		if ( null == locks) return;
		locks.clear();
		if (locksLists.size() > MAXIMUM_CACHE ) return;
		if ( locksLists.contains(locks)) return;
		locksLists.push(locks);
	}	
	
	public  List<Put> getPutList() {
		List<Put> puts = null;
		if (putsLists.size() > MINIMUM_CACHE ) puts = putsLists.pop();
		if ( null != puts ) return puts;
		return new ArrayList<Put>(256);
	}
	
	public  void putPutsList(List<Put> puts ) {
		if ( null == puts) return;
		puts.clear();
		if (putsLists.size() > MAXIMUM_CACHE ) return;
		if ( putsLists.contains(puts)) return;
		putsLists.push(puts);
	}		
	
	public  List<byte[]> getByteArrList() {
		List<byte[]> bytesA = null;
		if (byteArrLists.size() > MINIMUM_CACHE ) bytesA = byteArrLists.pop();
		if ( null != bytesA ) return bytesA;
		return new ArrayList<byte[]>(32);
	}
	
	public  void putByteArrList(List<byte[]> bytesA ) {
		if ( null == bytesA) return;
		bytesA.clear();
		if (byteArrLists.size() > MAXIMUM_CACHE ) return;
		if ( byteArrLists.contains(bytesA)) return;
		byteArrLists.push(bytesA);
	}		
	
	public  List<NV> getNVList() {
		List<NV> nvs = null;
		if (nvLists.size() > MINIMUM_CACHE ) nvs = nvLists.pop();
		if ( null != nvs ) return nvs;
		return new ArrayList<NV>();
	}
	
	public  void putNVList(List<NV> nvs ) {
		if ( null == nvs) return;
		nvs.clear();
		if (nvLists.size() > MAXIMUM_CACHE ) return;
		if ( nvLists.contains(nvs)) return;
		nvLists.push(nvs);
	}		
	
	public  Map<Long, Short> getBytesList() {
		Map<Long,Short> lstB = null;
		if (blockMap.size() > MINIMUM_CACHE ) lstB = blockMap.pop();
		if ( null != lstB ) return lstB;
		return new HashMap<Long,Short>();
	}
	
	public  void putBytesList(Map<Long,Short> lstB ) {
		if ( null == lstB) return;
		lstB.clear();
		if (blockMap.size() > MAXIMUM_CACHE ) return;
		if ( blockMap.contains(lstB)) return;
		blockMap.push(lstB);
	}		
	

	public Map<Integer,byte[]> getByteBlockMap(){
		Map<Integer,byte[]> obj = null;
		if (blockMaps.size() > MINIMUM_CACHE ) obj = blockMaps.pop();
		if ( null != obj ) return obj;
		return new Hashtable<Integer,byte[]>();		
	}
	
	public void putByteBlockMap(Map<Integer,byte[]> obj){
		if ( null == obj) return;
		obj.clear();
		if (blockMaps.size() > MAXIMUM_CACHE ) return;
		if ( blockMaps.contains(obj)) return;
		blockMaps.push(obj);
	}	
	
	public Set<String> getStringSet(){
		Set<String> obj = null;
		if (stringSets.size() > MINIMUM_CACHE ) obj = stringSets.pop();
		if ( null != obj ) return obj;
		return new HashSet<String>();		
	}
	
	public void putStringSet(Set<String> obj){
		if ( null == obj) return;
		obj.clear();
		if (stringSets.size() > MAXIMUM_CACHE ) return;
		if ( stringSets.contains(obj)) return;
		stringSets.push(obj);
	}			
		
	public List<String> getStringList(){
		List<String> obj = null;
		if (stringLists.size() > MINIMUM_CACHE ) obj = stringLists.pop();
		if ( null != obj ) return obj;
		return new ArrayList<String>();		
	}
	
	public void putStringList(List<String> obj){
		if ( null == obj) return;
		obj.clear();
		if (stringLists.size() > MAXIMUM_CACHE ) return;
		if ( stringLists.contains(obj)) return;
		stringLists.push(obj);
	}		
	
	public List<Short> getShortList(){
		List<Short> obj = null;
		if (shortLists.size() > MINIMUM_CACHE ) obj = shortLists.pop();
		if ( null != obj ) return obj;
		return new ArrayList<Short>();		
	}
	
	public void putShortList(List<Short> obj){
		if ( null == obj) return;
		obj.clear();
		if (shortLists.size() > MAXIMUM_CACHE ) return;
		if ( shortLists.contains(obj)) return;
		shortLists.push(obj);
	}		
	
	public List<Integer> getIntegerList(){
		List<Integer> obj = null;
		if (integerLists.size() > MINIMUM_CACHE ) obj = integerLists.pop();
		if ( null != obj ) return obj;
		return new ArrayList<Integer>();		
	}
	
	public void putIntegerList(List<Integer> obj){
		if ( null == obj) return;
		obj.clear();
		if (integerLists.size() > MAXIMUM_CACHE ) return;
		if ( integerLists.contains(obj)) return;
		integerLists.push(obj);
	}			
	

	public Set<Long> getLongSet(){
		Set<Long> obj = null;
		if (longSets.size() > MINIMUM_CACHE ) obj = longSets.pop();
		if ( null != obj ) return obj;
		return new HashSet<Long>();		
	}
	
	public void putLongSet(Set<Long> obj){
		if ( null == obj) return;
		obj.clear();
		if (longSets.size() > MAXIMUM_CACHE ) return;
		if ( longSets.contains(obj)) return;
		longSets.push(obj);
	}
	
	public String getStatus() {
		StringBuilder sb = new StringBuilder(476);
		sb.append("<o>");
		sb.append("blockMap:").append(blockMap.size()).append('|');
		sb.append("blockMaps:").append(blockMaps.size()).append('|');
		sb.append("nvLists:").append(nvLists.size()).append('|');
		sb.append("stringSets:").append(stringSets.size()).append('|');
		sb.append("stringLists:").append(stringLists.size()).append('|');
		sb.append("shortLists:").append(shortLists.size()).append('|');
		sb.append("longSets:").append(longSets.size()).append('|');
		sb.append("integerLists:").append(integerLists.size());
		sb.append("</o>");
		return sb.toString();
	}
	
}
