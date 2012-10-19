
package com.bizosys.hsearch.byteutils;

import java.io.IOException;
import java.util.Collection;

public class SortedBytesUnsignedShort extends SortedByte<Integer>{

	private short MINIMUM_ALLOWED_LIMIT = 0;
	private int MAXIMUM_ALLOWED_LIMIT = new Integer(Short.MAX_VALUE) - (Short.MIN_VALUE - MINIMUM_ALLOWED_LIMIT);
	

	private static SortedBytesUnsignedShort singleton = new SortedBytesUnsignedShort();
	public static SortedBytesUnsignedShort getInstance() {
		return singleton;
	}
	
	private SortedBytesUnsignedShort() {
	}
	
	public SortedBytesUnsignedShort setMinimumValue(short minVal) {
		SortedBytesUnsignedShort newProcessor = new SortedBytesUnsignedShort();
		newProcessor.MINIMUM_ALLOWED_LIMIT = minVal;
		newProcessor.MAXIMUM_ALLOWED_LIMIT = new Integer(Short.MAX_VALUE) - (Short.MIN_VALUE - newProcessor.MINIMUM_ALLOWED_LIMIT);
		return newProcessor;
	}
	
	@Override
	public Integer getValueAt(byte[] bytes, int pos) {
		int val = Storable.getShort(pos*2, bytes) - Short.MIN_VALUE + MINIMUM_ALLOWED_LIMIT;
		return (val);
	}
	
	@Override
	public Integer getValueAt(byte[] bytes, int offset, int pos) {
		int val = Storable.getShort(pos*2 + offset, bytes) - Short.MIN_VALUE + MINIMUM_ALLOWED_LIMIT;
		return (val);
	}	
	
	@Override
	public void addAll(byte[] bytes, Collection<Integer>  vals) throws IOException {
		addAll(bytes, 0, vals);
	}
	
	@Override
	public void addAll(byte[] bytes, int offset, Collection<Integer>  vals) throws IOException {
		int total = (bytes.length - offset) / 4;
		for ( int pos=0; pos<total; pos++) {
			vals.add( Storable.getShort(pos*2 + offset, bytes) - Short.MIN_VALUE + MINIMUM_ALLOWED_LIMIT );
		}
	}	
	
	@Override
	public byte[] toBytes(Collection<Integer> sortedList, boolean clearList) throws IOException {

		int sortedListAT = sortedList.size();
		byte[] inputsB = new byte[sortedListAT * 2];
		
		int index = 0;
		
		for (Integer aVal : sortedList) {
			Short aValS = getShort(aVal);
			System.arraycopy(Storable.putShort(aValS), 0, inputsB, index * 2, 2);
			index++;
		}
		if ( clearList ) sortedList.clear();
		return inputsB;
	}
	
	public byte[] toBytesFromShorts(Collection<Short> sortedList, boolean clearList) throws IOException {

		int sortedListAT = sortedList.size();
		byte[] inputsB = new byte[sortedListAT * 2];
		
		int index = 0;
		for (Short aVal : sortedList) {
			aVal = getShort(aVal.intValue());
			System.arraycopy(Storable.putShort(aVal), 0, inputsB, index * 2, 2);
			index++;
		}
		return inputsB;
	}

	@Override
	public int getEqualToIndex(byte[] intB, Integer aVal) throws IOException {
		short matchNo = getShort(aVal);
		return getEqualToIndexShort(intB, 0, matchNo);
		
	}
	
	@Override
	public int getEqualToIndex(byte[] intB, int offset, Integer aVal) throws IOException {
		short matchNo = getShort(aVal);
		return getEqualToIndexShort(intB, offset, matchNo);
		
	}

	@Override
	public void getEqualToIndexes(byte[] intB, Integer aVal, Collection<Integer> matchings) throws IOException {
		
		if ( null == intB) return;

		short matchingNo = getShort(aVal);
		//System.out.println("Short Value : " + matchingNo);
		byte[] matchingNoB = Storable.putShort(matchingNo);

		int index = getEqualToIndexShort(intB, 0, matchingNo);
		//System.out.println("getEqualToIndex:" + index);
		if ( index == -1) return;
		
		int intBT = intB.length / 2;
		
		//Include all matching indexes from left
		matchings.add(index);
		//System.out.println("found:" + index);
		for ( int i=index-1; i>=0; i--) {
			int pos = i * 2;
			if ( intB[pos] !=  matchingNoB[0]) break;
			if ( intB[pos+1] !=  matchingNoB[1]) break;
			matchings.add(i);
		}
		
		//Include all matching indexes from right
		for ( int i=index+1; i<intBT; i++) {
			int pos = i * 2;
			if ( intB[pos] !=  matchingNoB[0]) break;
			if ( intB[pos+1] !=  matchingNoB[1]) break;
			matchings.add(i);	
			
		}
	}	
	
	@Override
	public void getGreaterThanIndexes(byte[] intB, Integer matchingNo, Collection<Integer> matchingPos) throws IOException {
		computeGTGTEQIndexes(intB, matchingNo, matchingPos, false);
	}	
	
	@Override
	public void getGreaterThanEuqalToIndexes(byte[] intB, Integer matchingNo, Collection<Integer> matchingPos) throws IOException {
		computeGTGTEQIndexes(intB, matchingNo, matchingPos, true);
	}	
	
	private void computeGTGTEQIndexes(byte[] intB, int matchingVal, Collection<Integer> matchingPos, boolean isEqualCheck) throws IOException {
		if ( intB == null ) return;
		
		short matchingNo = getShort(matchingVal);
		
		int intBT = intB.length / 2;
		
		int left = 0;
		int right = intBT - 1;
		int mid = ( right - left ) / 2;
		int newMid = -1;
		
		while ( true ) {
			int isSame = ( compare(intB, mid*2, matchingNo));
			/**
			System.out.println("isSame > left-mid-right : " + new Integer(isSame).toString() + " > " + 
					new Integer(left).toString() + "-" + 
					new Integer(mid).toString() + "-" + new Integer(right).toString());
			*/
			boolean includesMatching = (isEqualCheck) ? (isSame <= 0) : (isSame < 0);
			if ( includesMatching ) {
				for ( int i=mid; i<intBT; i++) matchingPos.add(i);
				for ( int i=mid-1; i>=left; i--) {
					isSame = ( compare(intB, i*2, matchingNo));
					includesMatching = (isEqualCheck) ? (isSame <= 0) : (isSame < 0);
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
		
		//Should never reach here
	}
	
	@Override
	public void getLessThanIndexes(byte[] intB, Integer matchingNo, Collection<Integer> matchingPos) throws IOException {
		computeLTLTEQIndexes(intB, matchingNo, matchingPos, false);
	}	
	
	@Override
	public void getLessThanEuqalToIndexes(byte[] intB, Integer matchingNo, Collection<Integer> matchingPos)  throws IOException {
		computeLTLTEQIndexes(intB, matchingNo, matchingPos, true);
	}	
	
	private void computeLTLTEQIndexes(byte[] intB, int matchingVal, Collection<Integer> matchingPos,  boolean isEqualCheck) throws IOException {
		if ( intB == null ) return;
		
		int intBT = intB.length / 2;
		short matchingNo = getShort(matchingVal);
		//System.out.println(matchingVal + " ~ " + matchingNo);
		
		int left = 0;
		int right = intBT - 1;
		int mid = ( right - left ) / 2;
		int newMid = -1;
		
		while ( true ) {
			int isSame = ( compare(intB, mid*2, matchingNo));
			
			/**
			System.out.println("isSame > left-mid-right : " + new Integer(isSame).toString() + " > " + 
					new Integer(left).toString() + "-" + 
					new Integer(mid).toString() + "-" + new Integer(right).toString());
			*/
			boolean includesMatching = (isEqualCheck) ? (isSame >= 0) : (isSame > 0); //matchNo > leftNo
			if ( includesMatching ) {
				for ( int i=mid; i>=left ; i--) matchingPos.add(i);
				
				for ( int i=mid+1; i<=right ; i++) {
					isSame = ( compare(intB, i*2, matchingNo));
					includesMatching = (isEqualCheck) ? (isSame >= 0) : (isSame > 0);
					//System.out.println("y:" + i + "--" + includesMatching + " ," + Storable.getShort(i*2, intB)) ;
					if (includesMatching ) matchingPos.add(i);
					else break;
				}
			}
			else {
				newMid = mid - ( mid - left) / 2;
				if ( newMid == mid && (left + 1) == mid ) newMid = left;
				right = mid;
			}
			
			if ( newMid == mid ) {
				mid = -1;
				break;
			}
			mid = newMid;
			if ( mid == -1) break;
		}
		
	}		
	
	private Short getShort(Integer aVal) throws IOException {
		if ( aVal > MAXIMUM_ALLOWED_LIMIT) throw new IOException("Suplied Value " + 
				aVal.toString() + " is greated than maximum limit : " + MAXIMUM_ALLOWED_LIMIT);

		if (aVal < MINIMUM_ALLOWED_LIMIT) throw new IOException("Suplied Value " + 
				aVal.toString() + " is less than minimum limit 0.");

		aVal = aVal + Short.MIN_VALUE;
		aVal = aVal - MINIMUM_ALLOWED_LIMIT;
		
		return aVal.shortValue();
	}
	
	private static int compare(byte[] inputB, int offset,short matchNo) {
		
		int leftNo = (inputB[offset] << 8) + (inputB[++offset] & 0xff);
		//System.out.println(matchNo + " supplied vs extracted " + leftNo + " @" + offset);

		if ( matchNo == leftNo) return 0;
		if ( matchNo < leftNo) return -1;
		return 1;
	}
		
	
	private static int getEqualToIndexShort(byte[] intB, int offset, short matchNo) throws IOException {
		
		int intBT = intB.length / 2;
		
		int left = 0;
		int right = intBT - 1;
		int mid = ( right - left ) / 2;
		int newMid = -1;
		
		while ( true ) {
			int isSame = ( compare(intB, mid*2 + offset, matchNo));
			/**
			System.out.println("isSame: left, mid, right : " + new Integer(isSame).toString() + " > " + 
					new Integer(left).toString() + "-" + new Integer(mid).toString() + "-" + 
					new Integer(right).toString());
			*/
			
			if ( isSame == 0 ) {
				//System.out.println("Returning Mid : " + mid);
				return mid;
			}
			if ( mid == left || mid == right) {
				mid = -1;
				break;
			}
			if ( isSame > 0 ) {
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

		
}
