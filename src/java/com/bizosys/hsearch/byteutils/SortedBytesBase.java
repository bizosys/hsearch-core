package com.bizosys.hsearch.byteutils;

import java.util.Collection;

public abstract class SortedBytesBase<T> extends SortedByte<T> {
	
	protected abstract int compare(byte[] inputB, int offset, T matchNo);
	
	protected int getEqualToIndex(byte[] inputBytes, int offset, T matchNo, int dataSize) {
		
		if ( null == inputBytes) return -1;
		int intBT = inputBytes.length / dataSize;
		if ( 0 == intBT) return -1;
		
		int left = 0;
		int right = intBT - 1;
		int mid = ( right - left ) / 2;
		int newMid = -1;
		
		while ( true ) {
			int isSame = ( compare(inputBytes, mid*dataSize + offset, matchNo));
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
	
	public void getEqualToIndexes(byte[] intB, T matchingNo, Collection<Integer> matchings, int dataSize) {
		if ( null == intB ) return;
		
		int index = getEqualToIndex(intB, 0, matchingNo, dataSize);
		if ( index == -1) return;

		int intBT = intB.length / dataSize;
		
		//Include all matching indexes from left
		matchings.add(index);
		//System.out.println("found:" + index);
		for ( int i=index-1; i>=0; i--) {
			if ( compare(intB,i * dataSize,matchingNo) != 0 )  break;
			//System.out.println("left:" + i);
			matchings.add(i);
		}
		
		//Include all matching indexes from right
		for ( int i=index+1; i<intBT; i++) {
			if ( compare(intB,i * dataSize,matchingNo) != 0 )  break;
			matchings.add(i);	
			
		}
	}			
	protected void computeGTGTEQIndexes(byte[] intB, T matchingNo, Collection<Integer> matchingPos, 
			boolean isEqualCheck, int dataSize) {
		
		if ( null == intB) return;
		int intBT = intB.length / dataSize;
		if ( 0 == intBT) return;
		
		int left = 0;
		int right = intBT - 1;
		int mid = ( right - left ) / 2;
		int newMid = -1;

		while ( true ) {
			int isSame = ( compare(intB, mid*dataSize, matchingNo));
			/**
			System.out.println("isSame:mid,left,right : " + new Integer(isSame).toString() + ":" + new Integer(mid).toString() + "/" + 
					new Integer(left).toString() + "/" + new Integer(right).toString());
			*/
			boolean includesMatching = (isEqualCheck) ? (isSame >= 0) : (isSame > 0);  //matchNo > foundno
			if ( includesMatching ) {
				for ( int i=mid; i<intBT; i++) matchingPos.add(i);

				for ( int i=mid-1; i>=left; i--) {
					isSame = ( compare(intB, i*dataSize, matchingNo));
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
	
	protected void computeLTLTEQIndexes(byte[] intB, T matchingNo, 
			Collection<Integer> matchingPos, boolean isEqualCheck, int dataSize) {

		if ( null == intB) return;
		int intBT = intB.length / dataSize;
		if ( 0 == intBT) return;
		
		int left = 0;
		int right = intBT - 1;
		int mid = ( right - left ) / 2;
		int newMid = -1;
		
		while ( true ) {
			int isSame = ( compare(intB, mid*dataSize, matchingNo));
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
					isSame = ( compare(intB, i*dataSize, matchingNo));
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
	protected void computeRangeIndexes(byte[] intB, T matchingValS, T matchingValE, 
			Collection<Integer> matchingPos, boolean isEqualCheck, int dataSize) {
		
		if ( null == intB) return;
		int intBT = intB.length / dataSize;
		if ( 0 == intBT) return;
		
		int left = 0;
		int right = intBT - 1;
		int mid = ( right - left ) / 2;
		int newMid = -1;
		
		while ( true ) {
			int isSameS = ( compare(intB, mid*dataSize, matchingValS));
			/**
			System.out.println("isSame:mid,left,right : " + new Integer(isSame).toString() + ":" + new Integer(mid).toString() + "/" + 
					new Integer(left).toString() + "/" + new Integer(right).toString());
			*/
			boolean includesMatchingS = (isEqualCheck) ? (isSameS >= 0) : (isSameS > 0);
			if ( includesMatchingS ) {
				int isSameE = -1;
				boolean includesMatchingE = false;
				
				for ( int i=mid; i<intBT; i++) {
					isSameE = ( compare(intB, i*dataSize, matchingValE));
					includesMatchingE = (isEqualCheck) ? (isSameE <= 0) : (isSameE < 0);
					if ( includesMatchingE ) {
						matchingPos.add(i);
					} else break;
				}

				for ( int i=mid-1; i>=left; i--) {
					isSameS = ( compare(intB, i*dataSize, matchingValS));
					isSameE = ( compare(intB, i*dataSize, matchingValE));
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
