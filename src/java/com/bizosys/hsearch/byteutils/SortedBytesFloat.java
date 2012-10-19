
package com.bizosys.hsearch.byteutils;

import java.io.IOException;
import java.util.Collection;
import java.util.List;

public final class SortedBytesFloat extends SortedByte<Float> {


	private static SortedBytesFloat singleton = new SortedBytesFloat();
	public static SortedByte<Float> getInstance() {
		return singleton;
	}
	
	private SortedBytesFloat() {
	}	

	@Override
	public Float getValueAt(byte[] bytes, int pos) {
		return Storable.getFloat(pos*4, bytes);
	}	
	
	@Override
	public Float getValueAt(byte[] bytes, int offset, int pos) {
		return Storable.getFloat(pos*4 + offset, bytes);
	}		
	
	@Override
	public void addAll(byte[] bytes, Collection<Float>  vals) throws IOException {
		addAll(bytes, 0, vals);
	}
	
	@Override
	public void addAll(byte[] bytes, int offset, Collection<Float>  vals) throws IOException {
		int total = (bytes.length - offset) / 4;
		for ( int pos=0; pos<total; pos++) {
			vals.add( Storable.getFloat(pos*4 + offset, bytes) );
		}
	}

		
	@Override
	public byte[] toBytes(Collection<Float> sortedList, boolean clearList) {

		int sortedListAT = sortedList.size();
		byte[] inputsB = new byte[sortedListAT * 4];
		
		int index = 0;
		for (Float aVal : sortedList) {
			System.arraycopy(Storable.putInt(Float.floatToIntBits(aVal)), 0, inputsB, index * 4, 4);
			index++;
		}
		return inputsB;
	}

	@Override
	public int getEqualToIndex(byte[] inputBytes, Float matchingNo) {
		return getEqualToIndex(inputBytes, 0, matchingNo);
	}
	

	@Override
	public int getEqualToIndex(byte[] inputBytes, int offset, Float matchingNo) {
		
		int intBT = inputBytes.length / 4;
		
		int left = 0;
		int right = intBT - 1;
		int mid = ( right - left ) / 2;
		int newMid = -1;
		
		while ( true ) {
			int isSame = ( compare(inputBytes, (mid*4 + offset), matchingNo));
			/**
			System.out.println("isSame:mid,left,right : " + new Integer(isSame).toString() + ":" + new Integer(mid).toString() + "/" + 
					new Integer(left).toString() + "/" + new Integer(right).toString());
			*/
			
			if ( isSame == 0 ) return mid;
			if ( mid == left || mid == right) {
				mid = -1;
				break;
			}
			if ( isSame < 0 ) {
				newMid = mid + ( right - mid ) / 2;
				if ( newMid == mid && (right -1) == mid ) newMid = right;
				left = mid;
			}
			else {
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
	public void getEqualToIndexes(byte[] intB, Float matchingNo, Collection<Integer> matchings) {
		if ( null == intB ) return;
		
		int index = getEqualToIndex(intB, 0, matchingNo);
		if ( index == -1) return;

		int intBT = intB.length / 4;
		
		//Include all matching indexes from left
		matchings.add(index);
		//System.out.println("found:" + index);
		for ( int i=index-1; i>=0; i--) {
			if ( compare(intB,i * 4,matchingNo) != 0 )  break;
			//System.out.println("left:" + i);
			matchings.add(i);
		}
		
		//Include all matching indexes from right
		for ( int i=index+1; i<intBT; i++) {
			if ( compare(intB,i * 4,matchingNo) != 0 )  break;
			matchings.add(i);	
			
		}
	}	
	
	@Override
	public void getGreaterThanIndexes(byte[] intB, Float matchingNoB, Collection<Integer> matchingPos) {
		computeGTGTEQIndexes(intB, matchingNoB, matchingPos, false);
	}	
	
	@Override
	public void getGreaterThanEuqalToIndexes(byte[] intB, Float matchingNoB, Collection<Integer> matchingPos) {
		computeGTGTEQIndexes(intB, matchingNoB, matchingPos, true);
	}	
	
	private void computeGTGTEQIndexes(byte[] intB, float matchingNoB, Collection<Integer> matchingPos, boolean isEqualCheck) {
		if ( intB == null ) return;
		
		int intBT = intB.length / 4;
		
		int left = 0;
		int right = intBT - 1;
		int mid = ( right - left ) / 2;
		int newMid = -1;
		
		while ( true ) {
			int isSame = ( compare(intB, mid*4, matchingNoB));
			/**
			System.out.println("isSame:mid,left,right : " + new Integer(isSame).toString() + ":" + new Integer(mid).toString() + "/" + 
					new Integer(left).toString() + "/" + new Integer(right).toString());
			*/
			boolean includesMatching = (isEqualCheck) ? (isSame >= 0) : (isSame > 0);
			if ( includesMatching ) {
				for ( int i=mid; i<intBT; i++) matchingPos.add(i);

				for ( int i=mid-1; i>=left; i--) {
					isSame = ( compare(intB, i*4, matchingNoB));
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
	
	public void getGreaterThanEuqalToValues(byte[] intB, float matchingNoB, List<Float> matchingVals) {
		computeGTGTEQValues(intB, matchingNoB, matchingVals, true);
	}		

	public void getGreaterThanValues(byte[] intB, Float matchingNoB, List<Float> matchingVals) {
		computeGTGTEQValues(intB, matchingNoB, matchingVals, false);
	}	

	
	private void computeGTGTEQValues(byte[] intB, float matchingNoB, List<Float> matchingVals, boolean isEqualCheck) {
		if ( intB == null ) return;
		
		int intBT = intB.length / 4;
		
		int left = 0;
		int right = intBT - 1;
		int mid = ( right - left ) / 2;
		int newMid = -1;
		
		while ( true ) {
			int isSame = ( compare(intB, mid*4, matchingNoB));
			/**
			System.out.println("isSame:mid,left,right : " + new Integer(isSame).toString() + ":" + new Integer(mid).toString() + "/" + 
					new Integer(left).toString() + "/" + new Integer(right).toString());
			*/
			boolean includesMatching = (isEqualCheck) ? (isSame >= 0) : (isSame > 0);
			if ( includesMatching ) {
				for ( int i=mid; i<intBT; i++) matchingVals.add(Storable.getFloat(i * 4, intB) );

				for ( int i=mid-1; i>=left; i--) {
					isSame = ( compare(intB, i*4, matchingNoB));
					includesMatching = (isEqualCheck) ? (isSame >= 0) : (isSame > 0);
					if ( includesMatching ) matchingVals.add(Storable.getFloat(i * 4, intB) );
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
	public void getLessThanIndexes(byte[] intB, Float matchingNoB, Collection<Integer> matchingPos) {
		computeLTLTEQIndexes(intB, matchingNoB, matchingPos, false);
	}	
	
	@Override
	public void getLessThanEuqalToIndexes(byte[] intB, Float matchingNoB, Collection<Integer> matchingPos) {
		computeLTLTEQIndexes(intB, matchingNoB, matchingPos, true);
	}	
	
	private void computeLTLTEQIndexes(byte[] intB, float matchingNoB, Collection<Integer> matchingPos, boolean isEqualCheck) {
		if ( intB == null ) return;
		
		int intBT = intB.length / 4;
		
		int left = 0;
		int right = intBT - 1;
		int mid = ( right - left ) / 2;
		int newMid = -1;
		
		while ( true ) {
			int isSame = ( compare(intB, mid*4, matchingNoB));
			/**
			System.out.println("isSame:mid,left,right : " + new Integer(isSame).toString() + ":" + new Integer(mid).toString() + "/" + 
					new Integer(left).toString() + "/" + new Integer(right).toString());
			*/
			boolean includesMatching = (isEqualCheck) ? (isSame > 0) : (isSame >= 0);
			if (includesMatching ) {
				newMid = mid - ( mid - left) / 2;
				if ( newMid == mid && (left + 1) == mid ) newMid = left;
				right = mid;
			}
			else {
				for ( int i=left; i<=mid ; i++) {
					isSame = (compare(intB, i*4, matchingNoB));
					includesMatching = (isEqualCheck) ? (isSame <= 0) : (isSame < 0);
					//System.out.println("x:" + i + "--" + includesMatching) ;
					if (includesMatching ) matchingPos.add(i);
					else break;
				}
				
				for ( int i=mid+1; i<=right ; i++) {
					isSame = ( compare(intB, i*4, matchingNoB));
					includesMatching = (isEqualCheck) ? (isSame <= 0) : (isSame < 0);
					//System.out.println("y:" + i + "--" + includesMatching) ;
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
	
	public void getLessThanValues(byte[] intB, float matchingNoB, List<Float> matchingVals) {
		computeLTLTEQValues(intB, matchingNoB, false, matchingVals);
	}	
	
	public void getLessThanEuqalToValues(byte[] intB, float matchingNoB, List<Float> matchingVals) {
		computeLTLTEQValues(intB, matchingNoB, true, matchingVals);
	}		
	
	private void computeLTLTEQValues(byte[] intB, float matchingNoB, boolean isEqualCheck, List<Float> matchingVals) {
		if ( intB == null ) return;
		
		int intBT = intB.length / 4;
		
		int left = 0;
		int right = intBT - 1;
		int mid = ( right - left ) / 2;
		int newMid = -1;
		
		while ( true ) {
			int isSame = ( compare(intB, mid*4, matchingNoB));
			/**
			System.out.println("isSame:mid,left,right : " + new Integer(isSame).toString() + ":" + new Integer(mid).toString() + "/" + 
					new Integer(left).toString() + "/" + new Integer(right).toString());
			*/
			boolean includesMatching = (isEqualCheck) ? (isSame > 0) : (isSame >= 0);
			if (includesMatching ) {
				newMid = mid - ( mid - left) / 2;
				if ( newMid == mid && (left + 1) == mid ) newMid = left;
				right = mid;
			}
			else {
				for ( int i=left; i<=mid ; i++) {
					isSame = (compare(intB, i*4, matchingNoB));
					includesMatching = (isEqualCheck) ? (isSame <= 0) : (isSame < 0);
					//System.out.println("x:" + i + "--" + includesMatching) ;
					if (includesMatching ) matchingVals.add(Storable.getFloat(i*4, intB));
					else break;
				}
				
				for ( int i=mid+1; i<=right ; i++) {
					isSame = ( compare(intB, i*4, matchingNoB));
					includesMatching = (isEqualCheck) ? (isSame <= 0) : (isSame < 0);
					//System.out.println("y:" + i + "--" + includesMatching) ;
					if (includesMatching ) matchingVals.add(Storable.getFloat(i*4, intB));
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
	
	private int compare(byte[] inputB, int offset, float outputF) {
		float val = Float.intBitsToFloat( Storable.getInt(offset, inputB) );
		if (val == outputF) return 0;
		if (val > outputF) return 1;
		return -1;
	}
		
}
