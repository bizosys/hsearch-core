
package com.bizosys.hsearch.byteutils;

import java.io.IOException;
import java.util.Collection;

public final class SortedBytesInteger extends SortedByte<Integer>{

	private static SortedBytesInteger singleton = new SortedBytesInteger();
	public static SortedByte<Integer> getInstance() {
		return singleton;
	}
	
	private SortedBytesInteger() {
	}
	
	@Override
	public Integer getValueAt(byte[] bytes, int pos) {
		return Storable.getInt(pos*4, bytes);
	}	
	
	@Override
	public Integer getValueAt(byte[] bytes, int offset, int pos) {
		return Storable.getInt(pos*4 + offset, bytes);
	}	
	
	@Override
	public void addAll(byte[] bytes, Collection<Integer>  vals) throws IOException {
		addAll(bytes, 0, vals);
	}
	
	@Override
	public void addAll(byte[] bytes, int offset, Collection<Integer>  vals) throws IOException {
		int total = (bytes.length - offset) / 4;
		for ( int pos=0; pos<total; pos++) {
			vals.add( Storable.getInt(pos*4 + offset, bytes) );
		}
	}	

	@Override
	public byte[] toBytes(Collection<Integer> sortedList, boolean clearList) {

		int sortedListAT = sortedList.size();
		byte[] inputsB = new byte[sortedListAT * 4];
		
		int index = 0;
		for (Integer aVal : sortedList) {
			System.arraycopy(Storable.putInt(aVal), 0, inputsB, index * 4, 4);
			index++;
		}
		
		if  ( clearList ) sortedList.clear();
		return inputsB;
	}
	
	@Override
	public int getEqualToIndex(byte[] inputBytes, Integer matchingNo) {
		return getEqualToIndex(inputBytes, 0, matchingNo);
	}
		
	
	@Override
	public int getEqualToIndex(byte[] inputBytes, int offset, Integer matchNo) {
		
		if ( null == inputBytes) return -1;
		int intBT = inputBytes.length / 4;
		if ( 0 == intBT) return -1;
		
		int left = 0;
		int right = intBT - 1;
		int mid = ( right - left ) / 2;
		int newMid = -1;
		
		while ( true ) {
			int isSame = ( compare(inputBytes, mid*4 + offset, matchNo));
			/**
			System.out.println("isSame: left, mid, right : " + new Integer(isSame).toString() + " > " + 
					new Integer(left).toString() + "-" + new Integer(mid).toString() + "-" + 
					new Integer(right).toString());
			*/
			
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
	public void getEqualToIndexes(byte[] intB, Integer matchNo, Collection<Integer> matchings) {

		if ( null == intB) return;
		int intBT = intB.length / 4;
		if ( 0 == intBT) return;
		
		byte[] matchingNoB = Storable.putInt(matchNo);
		
		int index = getEqualToIndex(intB, 0, matchNo);
		//System.out.println("index:" + index);
		if ( index == -1) return ;

		//Include all matching indexes from left
		matchings.add(index);
		//System.out.println("First Index:" + index);
		for ( int i=index-1; i>=0; i--) {
			int pos = i * 4;
			if ( intB[pos] !=  matchingNoB[0]) break;
			if ( intB[pos+1] !=  matchingNoB[1]) break;
			if ( intB[pos+2] !=  matchingNoB[2]) break;
			if ( intB[pos+3] !=  matchingNoB[3]) break;
			//System.out.println("left:" + i);
			matchings.add(i);
		}
		
		//Include all matching indexes from right
		for ( int i=index+1; i<intBT; i++) {
			int pos = i * 4;
			if ( intB[pos] !=  matchingNoB[0]) break;
			if ( intB[pos+1] !=  matchingNoB[1]) break;
			if ( intB[pos+2] !=  matchingNoB[2]) break;
			if ( intB[pos+3] !=  matchingNoB[3]) break;
			//System.out.println("right:" + i);
			matchings.add(i);	
			
		}
	}	
	
	public void getGreaterThanIndexes(byte[] intB, Integer matchingNo, Collection<Integer> matchingPos) {
		computeGTGTEQIndexes(intB, matchingNo, matchingPos, false);
	}

	
	public void getGreaterThanEuqalToIndexes(byte[] intB, Integer matchingNo, Collection<Integer> matchingPos) {
		computeGTGTEQIndexes(intB, matchingNo, matchingPos, true);
	}	
	
	private void computeGTGTEQIndexes(byte[] intB, int matchingNo, Collection<Integer> matchingPos, boolean isEqualCheck) {
		if ( null == intB) return;
		int intBT = intB.length / 4;
		if ( 0 == intBT) return;
		
		int left = 0;
		int right = intBT - 1;
		int mid = ( right - left ) / 2;
		int newMid = -1;

		while ( true ) {
			int isSame = ( compare(intB, mid*4, matchingNo));
			/**
			System.out.println("isSame:mid,left,right : " + new Integer(isSame).toString() + ":" + new Integer(mid).toString() + "/" + 
					new Integer(left).toString() + "/" + new Integer(right).toString());
			*/
			boolean includesMatching = (isEqualCheck) ? (isSame >= 0) : (isSame > 0);  //matchNo > foundno
			if ( includesMatching ) {
				for ( int i=mid; i<intBT; i++) matchingPos.add(i);

				for ( int i=mid-1; i>=left; i--) {
					isSame = ( compare(intB, i*4, matchingNo));
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
	public void getLessThanIndexes(byte[] intB, Integer matchingNo, Collection<Integer> matchingPos ) {
		computeLTLTEQIndexes(intB, matchingNo, matchingPos, false);
	}
	
	@Override
	public void getLessThanEuqalToIndexes(byte[] intB, Integer matchingNo, Collection<Integer> matchingPos) {
		computeLTLTEQIndexes(intB, matchingNo, matchingPos, true);
	}
	
	private void computeLTLTEQIndexes(byte[] intB, int matchingNo, Collection<Integer> matchingPos, boolean isEqualCheck) {

		if ( null == intB) return;
		int intBT = intB.length / 4;
		if ( 0 == intBT) return;
		
		int left = 0;
		int right = intBT - 1;
		int mid = ( right - left ) / 2;
		int newMid = -1;
		
		while ( true ) {
			int isSame = ( compare(intB, mid*4, matchingNo));
			/**
			System.out.println("isSame:mid,left,right : " + new Integer(isSame).toString() + ":" + new Integer(mid).toString() + "/" + 
					new Integer(left).toString() + "/" + new Integer(right).toString());
			*/
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
					isSame = ( compare(intB, i*4, matchingNo));
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
	
	

	private int compare(byte[] inputB, int offset, int matchNo) {
		
		//System.out.println(Storable.getInt(offset, inputB) + " supplied vs extracted " + matchNo + " @" + offset);
		
		int val = (inputB[offset] << 24 ) +  ( (inputB[++offset] & 0xff ) << 16 ) + 
				(  ( inputB[++offset] & 0xff ) << 8 ) + ( inputB[++offset] & 0xff );
		
		if ( val == matchNo) return 0;
		if (val > matchNo) return 1;
		return -1;
		
	}

	@Override
	public void getRangeIndexes(byte[] inputData, Integer matchNoStart,
			Integer matchNoEnd, Collection<Integer> matchings)
			throws IOException {
		computeRangeIndexes(inputData, matchNoStart, matchNoEnd, matchings, false);
		
	}

	@Override
	public void getRangeIndexesInclusive(byte[] inputData,
			Integer matchNoStart, Integer matchNoEnd,
			Collection<Integer> matchings) throws IOException {
		computeRangeIndexes(inputData, matchNoStart, matchNoEnd, matchings, true);
		
	}
	
	private void computeRangeIndexes(byte[] intB, int matchingValS, int matchingValE, Collection<Integer> matchingPos, boolean isEqualCheck) {
	
		if ( null == intB) return;
		int intBT = intB.length / 4;
		if ( 0 == intBT) return;
		
		int left = 0;
		int right = intBT - 1;
		int mid = ( right - left ) / 2;
		int newMid = -1;
		
		while ( true ) {
			int isSameS = ( compare(intB, mid*4, matchingValS));
			/**
			System.out.println("isSame:mid,left,right : " + new Integer(isSame).toString() + ":" + new Integer(mid).toString() + "/" + 
					new Integer(left).toString() + "/" + new Integer(right).toString());
			*/
			boolean includesMatchingS = (isEqualCheck) ? (isSameS >= 0) : (isSameS > 0);
			if ( includesMatchingS ) {
				int isSameE = -1;
				boolean includesMatchingE = false;
				
				for ( int i=mid; i<intBT; i++) {
					isSameE = ( compare(intB, i*4, matchingValE));
					includesMatchingE = (isEqualCheck) ? (isSameE <= 0) : (isSameE < 0);
					if ( includesMatchingE ) {
						matchingPos.add(i);
					} else break;
				}

				for ( int i=mid-1; i>=left; i--) {
					isSameS = ( compare(intB, i*4, matchingValS));
					isSameE = ( compare(intB, i*4, matchingValE));
					includesMatchingS = (isEqualCheck) ? (isSameS >= 0) : (isSameS > 0);
					includesMatchingE = (isEqualCheck) ? (isSameE <= 0) : (isSameE < 0);
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
