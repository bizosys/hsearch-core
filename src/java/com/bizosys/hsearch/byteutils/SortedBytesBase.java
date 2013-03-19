package com.bizosys.hsearch.byteutils;
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

public abstract class SortedBytesBase<T> implements ISortedByte<T> {
	
	public static class Reference {
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
	
	protected abstract int compare(byte[] inputB, int offset, T matchNo);
	
	@Override
	public final ISortedByte<T> parse(byte[] bytes) throws IOException {
		this.inputBytes = bytes;
		this.offset = 0;
		this.length = ( null == bytes) ? 0 : bytes.length;
		return this;
	}

	@Override
	public final ISortedByte<T> parse(byte[] bytes, int offset, int length) throws IOException {
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
		for ( int index=0; index<total; index++) {
			vals.add( getValueAt(index));
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
		
		for ( int index=0; index<total; index++) {
			vals.add( getValueAt(index));
		}
		return vals;
	}
	
	@Override
	public int getEqualToIndex(T matchNo) throws IOException {
		
		int totalEntities = getSize();
		if ( 0 == totalEntities) return -1;
		
		int left = 0;
		int right = totalEntities - 1;
		int mid = ( right - left ) / 2;
		int newMid = -1;
		int isSame = -1;
		
		//Mid does not make sense for elements less than 3
		switch ( totalEntities) { 
			case 1:
				//No need to apply the MID principle
				isSame = ( compare(inputBytes, offset, matchNo));
				if ( isSame == 0 ) return 0;
				else return -1;
			case 2:
				isSame = ( compare(inputBytes, offset, matchNo));
				if ( isSame == 0 ) return 0;
				isSame = ( compare(inputBytes, offset + dataSize, matchNo));
				if ( isSame == 0 ) return 1;
				else return -1;
			default:
		}
		
		while ( true ) {
			isSame = ( compare(inputBytes, this.offset + mid*dataSize, matchNo));
			if ( isSame == 0 ) return mid;
			if ( mid == left || mid == right) {
				mid = -1;
				break;
			}
			if ( isSame < 0 ) { //Smaller
				newMid = mid + ( right - mid ) / 2;
				if ( newMid == mid && (right -1) == mid ) newMid = right;
				left = mid;
			}
			else { //Bigger
				newMid = left + ( mid - left ) / 2;
				right = mid;
			}
			
			if ( newMid == mid ) {
				mid = -1;
				break;
			}
			mid = newMid;
		}
		
		//Should never reach here
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
		
		int index = getEqualToIndex(matchingNo);
		if ( index == -1) return;

		int intBT = getSize();
		
		//Include all matching indexes from left
		matchings.add(index);
		for ( int i=index-1; i>=0; i--) {
			if ( compare(inputBytes, this.offset + i * dataSize, matchingNo) != 0 )  break;
			matchings.add(i);
		}
		
		//Include all matching indexes from right
		for ( int i=index+1; i<intBT; i++) {
			if ( compare(inputBytes, this.offset + i * dataSize, matchingNo) != 0 )  break;
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

	protected void computeGTGTEQIndexes(T matchingNo, Collection<Integer> matchingPos, boolean isEqualCheck) throws IOException {
		
		int totalSize = getSize();
		if (  totalSize <= 0 ) return;
		
		int left = 0;
		int right = totalSize - 1;
		int mid = ( right - left ) / 2;
		int newMid = -1;

		while ( true ) {
			int isSame = ( compare(inputBytes, this.offset + mid*dataSize, matchingNo));
			boolean includesMatching = (isEqualCheck) ? (isSame >= 0) : (isSame > 0);  //matchNo > foundno
			if ( includesMatching ) {
				for ( int i=mid; i<totalSize; i++) matchingPos.add(i);

				for ( int i=mid-1; i>=left; i--) {
					isSame = ( compare(inputBytes, this.offset +  i*dataSize, matchingNo));
					includesMatching = (isEqualCheck) ? (isSame >= 0) : (isSame > 0);
					if ( includesMatching ) matchingPos.add(i);
					else break;
				}
			} else {
				newMid = mid + ( right - mid ) / 2;
				if ( newMid == mid && (right -1) == mid ) newMid = right;
				left = mid;
			}
			
			if ( newMid == mid ) {
				mid = -1;
				break;
			}
			mid = newMid;
			if ( mid < 0) break;
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
	
	protected void computeLTLTEQIndexes(T matchingNo, Collection<Integer> matchingPos, boolean isEqualCheck) throws IOException {

		int totalSize = getSize();
		if ( totalSize <= 0 ) return;
		
		int left = 0;
		int right = totalSize - 1;
		int mid = ( right - left ) / 2;
		int newMid = -1;
		
		while ( true ) {
			int isSame = ( compare(inputBytes, this.offset + mid*dataSize, matchingNo));
			boolean includesMatching = (isEqualCheck) ? (isSame <= 0) : (isSame < 0);
			if (! includesMatching ) {
				newMid = mid - ( mid - left) / 2;
				if ( newMid == mid && (left + 1) == mid ) newMid = left;
				right = mid;
			}
			else {
				//All values are lower here.
				for ( int i=mid; i>=left; i--) matchingPos.add(i);
				
				for ( int i=mid+1; i<=right ; i++) {
					isSame = ( compare(inputBytes, this.offset + i*dataSize, matchingNo));
					includesMatching = (isEqualCheck) ? (isSame <= 0) : (isSame < 0);
					if (includesMatching ) matchingPos.add(i);
					else break;
				}
				
			}
			
			if ( newMid == mid ) {
				mid = -1;
				break;
			}
			mid = newMid;
			if ( mid == -1) break;
		}
	}		
	
	
	@Override
	public Collection<Integer> getRangeIndexes(T matchNoStart, T matchNoEnd) throws IOException {
		Collection<Integer> matchingPos = new ArrayList<Integer>();
		getRangeIndexes(matchNoStart, matchNoEnd, matchingPos);
		return matchingPos;
		
	}

	@Override
	public void getRangeIndexes(T matchNoStart, T matchNoEnd, Collection<Integer> matchings) throws IOException {
		computeRangeIndexes(matchNoStart, matchNoEnd, false, false, matchings);		
	}
	
	@Override
	public Collection<Integer> getRangeIndexesInclusive(T matchNoStart, T matchNoEnd) throws IOException {
		Collection<Integer> matchingPos = new ArrayList<Integer>();
		getRangeIndexesInclusive(matchNoStart, matchNoEnd, matchingPos);
		return matchingPos;
	}
	
	public void getRangeIndexesInclusive(T matchNoStart, T matchNoEnd, Collection<Integer> matchings) throws IOException {
		computeRangeIndexes(matchNoStart, matchNoEnd, true, true, matchings);		
	}
	
	@Override
	public Collection<Integer> getRangeIndexesInclusive(T matchNoStart, boolean startMatch, T matchNoEnd, boolean endMatch) throws IOException {
		Collection<Integer> matchingPos = new ArrayList<Integer>();
		getRangeIndexesInclusive(matchNoStart, startMatch, matchNoEnd, endMatch, matchingPos);		
		return matchingPos;
	}

	@Override
	public void getRangeIndexesInclusive(T matchNoStart, boolean startMatch, T matchNoEnd, boolean endMatch, Collection<Integer> matchings) throws IOException {
		computeRangeIndexes(matchNoStart, matchNoEnd, startMatch, endMatch, matchings);		
	}
	
	protected void computeRangeIndexes(T matchingValS, T matchingValE, 
			boolean isStartInclusive, boolean isEndInclusive, Collection<Integer> matchingPos) throws IOException  {
		
		int size = this.getSize();
		if ( size <= 0) return;
		
		int left = 0;
		int right = size - 1;
		int mid = ( right - left ) / 2;
		int newMid = -1;
		
		while ( true ) {
			int isSameS = ( compare(inputBytes, this.offset + mid*dataSize, matchingValS));
			/**
			System.out.println("isSame:mid,left,right : " + new Integer(isSame).toString() + ":" + new Integer(mid).toString() + "/" + 
					new Integer(left).toString() + "/" + new Integer(right).toString());
			*/
			boolean includesMatchingS = (isStartInclusive) ? (isSameS >= 0) : (isSameS > 0);
			if ( includesMatchingS ) {
				int isSameE = -1;
				boolean includesMatchingE = false;
				
				for ( int i=mid; i<size; i++) {
					isSameE = ( compare(inputBytes, this.offset + i*dataSize, matchingValE));
					includesMatchingE = (isEndInclusive) ? (isSameE <= 0) : (isSameE < 0);
					if ( includesMatchingE ) {
						matchingPos.add(i);
					} else break;
				}

				for ( int i=mid-1; i>=left; i--) {
					isSameS = ( compare(inputBytes, this.offset + i*dataSize, matchingValS));
					isSameE = ( compare(inputBytes, this.offset + i*dataSize, matchingValE));
					includesMatchingS = (isStartInclusive) ? (isSameS >= 0) : (isSameS > 0);
					includesMatchingE = (isEndInclusive) ? (isSameE <= 0) : (isSameE < 0);
					if ( includesMatchingS && includesMatchingE)  matchingPos.add(i);
					if ( !(includesMatchingS || includesMatchingE) ) break;
				}
			} else {
				newMid = mid + ( right - mid ) / 2;
				if ( newMid == mid && (right -1) == mid ) newMid = right;
				left = mid;
			}
			
			if ( newMid == mid ) {
				mid = -1;
				break;
			}
			mid = newMid;
			if ( mid < 0) break;
		}
	}		
	
}
