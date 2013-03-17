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
package com.bizosys.hsearch.byteutils;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import com.google.protobuf.InvalidProtocolBufferException;

public class SortedBytesBooleanCompressed extends SortedBytesBase<Boolean>{

	public static ISortedByte<Boolean> getInstance() {
		return new SortedBytesBooleanCompressed();
	}
	
	List<Boolean> parsedBooleans = null;
	private SortedBytesBooleanCompressed() {
		this.dataSize = -1;
	}

	@Override
	public byte[] toBytes(Collection<Boolean> sortedCollection) throws IOException {
		ByteArrays.ArrayBool.Builder booleanBuilder = ByteArrays.ArrayBool.newBuilder();
		booleanBuilder.addAllVal(sortedCollection);
		return booleanBuilder.build().toByteArray();
	}

	@Override
	public int getSize() throws IOException {
		if ( null == parsedBooleans ) parse();
		if ( null == parsedBooleans ) return 0;
		return this.parsedBooleans.size();
		
	}

	@Override
	public void addAll(Collection<Boolean> vals) throws IOException {
		if ( null != this.parsedBooleans ) parse();
		if ( null == this.parsedBooleans ) this.parsedBooleans = new ArrayList<Boolean>(); 
		this.parsedBooleans.addAll(vals);
	}

	@Override
	public Boolean getValueAt(int pos) throws IndexOutOfBoundsException {
		initialize();
		return this.parsedBooleans.get(pos);
	}

	@Override
	public int getEqualToIndex(Boolean matchNo) throws IOException {
		initialize();

		boolean matchNoL = matchNo.booleanValue();
		int pos = -1;
		for (boolean entity : this.parsedBooleans) {
			pos++;
			if ( matchNoL == entity) return pos;
		}
		return -1;
	}

	@Override
	public void getEqualToIndexes(Boolean matchNo, Collection<Integer> matchings) throws IOException {

		initialize();

		boolean matchNoL = matchNo.booleanValue();
		int pos = -1;
		for (boolean entity : this.parsedBooleans) {
			pos++;
			if ( matchNoL == entity) matchings.add(pos);
		}
	}

	@Override
	public void getGreaterThanIndexes(Boolean matchingNo, Collection<Integer> matchingPos) throws IOException {
		throw new RuntimeException("Not implemented");
	}

	@Override
	public void getGreaterThanEqualToIndexes(Boolean matchingNo, Collection<Integer> matchingPos) throws IOException {
		throw new RuntimeException("Not implemented");
	}

	@Override
	public void getLessThanIndexes(Boolean matchingNo, Collection<Integer> matchingPos) throws IOException {
		throw new RuntimeException("Not implemented");
	}

	@Override
	public void getLessThanEqualToIndexes(Boolean matchingNo, Collection<Integer> matchingPos) throws IOException {
		throw new RuntimeException("Not implemented");
	}

	@Override
	public void getRangeIndexes(Boolean matchNoStart, Boolean matchNoEnd, Collection<Integer> matchings) throws IOException {
		throw new RuntimeException("Not implemented");
	}

	@Override
	public void getRangeIndexesInclusive(Boolean matchNoStart, Boolean matchNoEnd, Collection<Integer> matchings) throws IOException {
		throw new RuntimeException("Not implemented");
	}
	
	private void parse() throws IndexOutOfBoundsException {
		if ( null == this.inputBytes) return;
		
		byte[] bytesSubset = null;
		if ( offset == 0 && length == this.inputBytes.length) {
			bytesSubset = this.inputBytes;
		} else {
			bytesSubset = new byte[length];
			System.arraycopy(this.inputBytes, offset, bytesSubset, 0, this.length);
		}
		
		try {
			parsedBooleans = ByteArrays.ArrayBool.parseFrom(bytesSubset).getValList();
		} catch (InvalidProtocolBufferException e) {
			e.printStackTrace(System.err);
			throw new IndexOutOfBoundsException(e.getMessage());
		}
	}

	public void initialize() throws IndexOutOfBoundsException {
		if ( null == parsedBooleans ) parse();
		if ( null == parsedBooleans ) throw new IndexOutOfBoundsException("SortedBytesBoolean - No data exists");
	}

	@Override
	protected int compare(byte[] inputB, int offset, Boolean matchNo) {
		throw new RuntimeException("Not implemented");
	}

}
