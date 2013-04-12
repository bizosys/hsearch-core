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

public final class SortedBytesLongCompressed extends SortedBytesBase<Long>{

	public static ISortedByte<Long> getInstance() {
		return new SortedBytesLongCompressed();
	}
	
	List<Long> parsedLongs = null;
	private SortedBytesLongCompressed() {
		this.dataSize = -1;
	}

	@Override
	public final byte[] toBytes(final Collection<Long> sortedCollection) throws IOException {
		ByteArrays.ArrayLong.Builder longBuilder = ByteArrays.ArrayLong.newBuilder();
		longBuilder.addAllVal(sortedCollection);
		return longBuilder.build().toByteArray();
	}

	@Override
	public final int getSize() throws IOException {
		if ( null == parsedLongs ) parse();
		if ( null == parsedLongs ) return 0;
		return this.parsedLongs.size();
		
	}

	@Override
	public final void addAll(final Collection<Long> vals) throws IOException {
		if ( null != this.parsedLongs ) parse();
		if ( null == this.parsedLongs ) this.parsedLongs = new ArrayList<Long>(); 
		this.parsedLongs.addAll(vals);
	}

	@Override
	public final Long getValueAt(final int pos) throws IndexOutOfBoundsException {
		initialize();
		return this.parsedLongs.get(pos);
	}

	@Override
	public final int getEqualToIndex(final Long matchNo) throws IOException {
		initialize();

		long matchNoL = matchNo.longValue();
		int pos = -1;
		for (long entity : this.parsedLongs) {
			pos++;
			if ( matchNoL == entity) return pos;
		}
		return -1;
	}

	@Override
	public final void getEqualToIndexes(final Long matchNo, final Collection<Integer> matchings) throws IOException {

		initialize();

		long matchNoL = matchNo.longValue();
		int pos = -1;
		for (long entity : this.parsedLongs) {
			pos++;
			if ( matchNoL == entity) matchings.add(pos);
		}
	}

	@Override
	public final void getNotEqualToIndexes(final Long matchNo, final Collection<Integer> matchings) throws IOException {

		initialize();

		long matchNoL = matchNo.longValue();
		int pos = -1;
		for (long entity : this.parsedLongs) {
			pos++;
			if ( matchNoL != entity) matchings.add(pos);
		}
	}

	@Override
	public final void getGreaterThanIndexes(final Long matchingNo, final Collection<Integer> matchingPos) throws IOException {
		initialize();

		long matchNoL = matchingNo.longValue();
		int pos = -1;
		for (long entity : this.parsedLongs) {
			pos++;
			if ( matchNoL < entity) matchingPos.add(pos);
		}
	}

	@Override
	public final void getGreaterThanEqualToIndexes(final Long matchingNo, 
		final Collection<Integer> matchingPos) throws IOException {
		
		initialize();

		long matchNoL = matchingNo.longValue();
		int pos = -1;
		for (long entity : this.parsedLongs) {
			pos++;
			if ( matchNoL <= entity) matchingPos.add(pos);
		}
	}

	@Override
	public final void getLessThanIndexes(final Long matchingNo, 
		final Collection<Integer> matchingPos) throws IOException {
		
		initialize();

		long matchNoL = matchingNo.longValue();
		int pos = -1;
		for (long entity : this.parsedLongs) {
			pos++;
			if ( matchNoL > entity) matchingPos.add(pos);
		}
	}

	@Override
	public final void getLessThanEqualToIndexes(final Long matchingNo, 
		final Collection<Integer> matchingPos) throws IOException {

		initialize();

		long matchNoL = matchingNo.longValue();
		int pos = -1;
		for (long entity : this.parsedLongs) {
			pos++;
			if ( matchNoL >= entity) matchingPos.add(pos);
		}
	}

	@Override
	public final void getRangeIndexes(final Long matchNoStart, final Long matchNoEnd, 
			final Collection<Integer> matchings) throws IOException {

		initialize();

		long matchNoSL = matchNoStart.longValue();
		long matchNoEL = matchNoEnd.longValue();
		int pos = -1;
		for (long entity : this.parsedLongs) {
			pos++;
			if ( matchNoSL > entity && matchNoEL < entity ) matchings.add(pos);
		}
	}

	@Override
	public final void getRangeIndexesInclusive(final Long matchNoStart, 
		final Long matchNoEnd, final Collection<Integer> matchings) throws IOException {
		
		initialize();

		long matchNoSL = matchNoStart.longValue();
		long matchNoEL = matchNoEnd.longValue();
		int pos = -1;
		for (long entity : this.parsedLongs) {
			pos++;
			if ( matchNoSL >= entity && matchNoEL <= entity ) matchings.add(pos);
		}
	}
	
	private final void parse() throws IndexOutOfBoundsException {
		if ( null == this.inputBytes) return;
		
		byte[] bytesSubset = null;
		if ( offset == 0 && length == this.inputBytes.length) {
			bytesSubset = this.inputBytes;
		} else {
			bytesSubset = new byte[length];
			System.arraycopy(this.inputBytes, offset, bytesSubset, 0, this.length);
		}
		
		try {
			parsedLongs = ByteArrays.ArrayLong.parseFrom(bytesSubset).getValList();
		} catch (InvalidProtocolBufferException e) {
			e.printStackTrace(System.err);
			throw new IndexOutOfBoundsException(e.getMessage());
		}
	}

	public final void initialize() throws IndexOutOfBoundsException {
		if ( null == parsedLongs ) parse();
		if ( null == parsedLongs ) throw new IndexOutOfBoundsException("SortedBytesLongCompressed - No data exists");
	}

	@Override
	protected final int compare(final byte[] inputB, final int offset, final Long matchNo) {
		throw new RuntimeException("Not implemented");
	}

}
