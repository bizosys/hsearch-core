package com.bizosys.hsearch.byteutils.vs;
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
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import com.bizosys.hsearch.byteutils.ISortedByte;

public abstract class PositionalBytesBase<T> implements ISortedByte<T> {
	
	public static final class Reference {
		public int offset;
		public int length;

		public Reference() {
		}
		
		public Reference(final int offset, final int length ) {
			this.offset = offset;
			this.length = length;
		}
		
		public void set(final int offset, final int length ) {
			this.offset = offset;
			this.length = length;
		}
		
	}
	
	protected byte[] inputBytes = null;
	protected int offset = 0;
	protected int length = -1;
	protected int dataSize = -1;
	
	public T defaultValue = null;
	public byte[] defaultValueB = null;

	public abstract int compare(byte[] inputB, int offset, T matchNo);
	public abstract boolean isEqual(T firsValue, T secondValue);
	
	@Override
	public ISortedByte<T> parse(final byte[] bytes) throws IOException {
		this.inputBytes = bytes;
		this.offset = 0;
		this.length = ( null == bytes) ? 0 : bytes.length;
		return this;
	}

	@Override
	public ISortedByte<T> parse(final byte[] bytes, final int offset, final int length) throws IOException {
		this.inputBytes = bytes;
		this.offset = offset;
		this.length = length;
		return this;
	}
	
	@Override
	public int getSize() throws IOException {
		if ( null == this.inputBytes) return 0;
		int total = this.length / dataSize;
		if ( total < 0 ) throw new IOException("Invalid size, offset is out of range. Length, " + 
			this.length + " , Offset "  + this.offset + " , Size " + dataSize);
		return total;
	}

	@Override
	public void addAll(Collection<T> vals) throws IOException {
		if ( null == this.inputBytes ) return;
		int total = getSize();
		T value = null;
		for ( int index=0; index<total; index++) {
			value = getValueAt(index);
			if(isEqual(defaultValue, value))
				continue;
			vals.add(value);
		}
	}
	
	@Override
	public final Collection<T> values() throws IOException {
		List<T> vals = new ArrayList<T>();
		return values(vals);
	}
	
	@Override
	public  Collection<T> values(Collection<T> vals) throws IOException {
		if ( null == this.inputBytes ) return vals;
		int total = getSize();
		T value = null;
		
		for ( int index=0; index<total; index++) {
			value = getValueAt(index);
			if(defaultValue == value)
				continue;
			vals.add( value );
		}
		return vals;
	}
	
	@Override
	public int getEqualToIndex(T matchNo) throws IOException {
		
		int totalEntities = getSize();
		if ( 0 == totalEntities) return -1;
		
		for (int i = 0; i < totalEntities; i++) {
			if(compare(inputBytes, offset + i * dataSize, matchNo) == 0)
				return i;
		}
		return -1;
	}	
	
	@Override
	public Collection<Integer> getEqualToIndexes(final T matchNo) throws IOException  {
		Collection<Integer> matchingPos = new ArrayList<Integer>();
		getEqualToIndexes(matchNo, matchingPos);
		return matchingPos;
	}
	
	
	@Override
	public void getEqualToIndexes(final T matchingNo, final Collection<Integer> matchings) throws IOException {
		
		int intBT = getSize();
		for (int i = 0; i < intBT; i++) {
			if(compare(inputBytes, offset + i * dataSize, matchingNo) == 0)
				matchings.add(i);
		}
	}			

	@Override
	public void getNotEqualToIndexes(final T matchingNo, final Collection<Integer> matchings) throws IOException {
		
		int intBT = getSize();
		for (int i = 0; i < intBT; i++) {
			
			if(compare(inputBytes, offset + i * dataSize, matchingNo) == 0)
				continue;
			
			matchings.add(i);
		}
	}

	@Override
	public Collection<Integer> getGreaterThanIndexes(T matchNo) throws IOException {
		Collection<Integer> matchingPos = new ArrayList<Integer>();
		getGreaterThanIndexes(matchNo, matchingPos);
		return matchingPos;
	}
	
	@Override
	public void getGreaterThanIndexes(T matchNo, Collection<Integer> matchingPos) throws IOException {
		this.computeGTGTEQIndexes(matchNo, matchingPos, false);
	}

	@Override
	public Collection<Integer> getGreaterThanEqualToIndexes(T matchNo) throws IOException {
		Collection<Integer> matchingPos = new ArrayList<Integer>();
		getGreaterThanEqualToIndexes(matchNo, matchingPos);
		return matchingPos;
	}
	
	@Override
	public void getGreaterThanEqualToIndexes(T matchNo, Collection<Integer> matchingPos) throws IOException {
		this.computeGTGTEQIndexes(matchNo, matchingPos, true);
	}

	protected final void computeGTGTEQIndexes(final T matchingNo, final Collection<Integer> matchingPos, final boolean isEqualCheck) throws IOException {
		
		int totalSize = getSize();
		if (  totalSize <= 0 ) return;
		int isSame = -1;
		boolean includeMacthing = false;
		
		for (int i = 0; i < totalSize; i++) {
			isSame = compare(inputBytes, offset + i * dataSize, matchingNo);
			includeMacthing = (isEqualCheck) ? isSame >= 0 : isSame > 0;
			if(includeMacthing)
				matchingPos.add(i);
		}
	}
	
	@Override
	public Collection<Integer> getLessThanIndexes(T matchingNo) throws IOException {
		Collection<Integer> matchingPos = new ArrayList<Integer>();
		getLessThanIndexes(matchingNo,matchingPos) ;
		return matchingPos;
	}

	@Override
	public void getLessThanIndexes(T matchingNo, Collection<Integer> matchingPos ) throws IOException {
		computeLTLTEQIndexes(matchingNo, matchingPos, false);
	}

	@Override
	public Collection<Integer> getLessThanEqualToIndexes(T matchingNo) throws IOException {
		Collection<Integer> matchingPos = new ArrayList<Integer>();
		getLessThanEqualToIndexes(matchingNo,matchingPos) ;
		return matchingPos;
	}

	@Override
	public void getLessThanEqualToIndexes(T matchingNo, Collection<Integer> matchingPos) throws IOException {
		computeLTLTEQIndexes(matchingNo, matchingPos, true);
	}
	
	protected final void computeLTLTEQIndexes(final T matchingNo, 
		final Collection<Integer> matchingPos, final boolean isEqualCheck) throws IOException {

		int totalSize = getSize();
		if ( totalSize <= 0 ) return;

		int isSame = -1;
		boolean includeMacthing = false;
		
		for (int i = 0; i < totalSize; i++) {
			isSame = compare(inputBytes, offset + i * dataSize, matchingNo);
			includeMacthing = (isEqualCheck) ? isSame <= 0 : isSame < 0;
			if(includeMacthing)
				matchingPos.add(i);
		}		
	}		
	
	
	@Override
	public Collection<Integer> getRangeIndexes(final T matchNoStart, final T matchNoEnd) throws IOException {
		Collection<Integer> matchingPos = new ArrayList<Integer>();
		getRangeIndexes(matchNoStart, matchNoEnd, matchingPos);
		return matchingPos;
		
	}

	@Override
	public void getRangeIndexes(final T matchNoStart, final T matchNoEnd, final Collection<Integer> matchings) throws IOException {
		computeRangeIndexes(matchNoStart, matchNoEnd, false, false, matchings);		
	}
	
	@Override
	public Collection<Integer> getRangeIndexesInclusive(final T matchNoStart, final T matchNoEnd) throws IOException {
		Collection<Integer> matchingPos = new ArrayList<Integer>();
		getRangeIndexesInclusive(matchNoStart, matchNoEnd, matchingPos);
		return matchingPos;
	}
	
	public void getRangeIndexesInclusive(final T matchNoStart, final T matchNoEnd, final Collection<Integer> matchings) throws IOException {
		computeRangeIndexes(matchNoStart, matchNoEnd, true, true, matchings);		
	}
	
	@Override
	public Collection<Integer> getRangeIndexesInclusive(final T matchNoStart, 
			final boolean startMatch, final T matchNoEnd, final boolean endMatch) throws IOException {
		Collection<Integer> matchingPos = new ArrayList<Integer>();
		getRangeIndexesInclusive(matchNoStart, startMatch, matchNoEnd, endMatch, matchingPos);		
		return matchingPos;
	}

	@Override
	public void getRangeIndexesInclusive(final T matchNoStart, final boolean startMatch, 
			final T matchNoEnd, final boolean endMatch, final Collection<Integer> matchings) throws IOException {
		computeRangeIndexes(matchNoStart, matchNoEnd, startMatch, endMatch, matchings);		
	}
	
	protected void computeRangeIndexes(final T matchingValS, final T matchingValE, 
			final boolean isStartInclusive, final boolean isEndInclusive, final Collection<Integer> matchingPos) throws IOException  {

		int totalSize = getSize();
		if ( totalSize <= 0 ) return;

		int isSameS = -1;
		int isSameE = -1;
		boolean includeMacthingS = false;
		boolean includeMacthingE = false;
		
		for (int i = 0; i < totalSize; i++) {
			isSameS = compare(inputBytes, offset + i * dataSize, matchingValS);
			isSameE = compare(inputBytes, offset + i * dataSize, matchingValE);
			includeMacthingS = (isStartInclusive) ? isSameS >= 0 : isSameS > 0;
			includeMacthingE = (isEndInclusive) ? isSameE <= 0 : isSameE < 0;
			if(includeMacthingS && includeMacthingE)
				matchingPos.add(i);
		}		
	}		
	
}
