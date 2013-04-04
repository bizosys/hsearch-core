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

import java.util.ArrayList;
import java.util.List;

public class FloatByteOps {

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		byte[] bytes = new byte[400];
		for ( int i=0; i<100; i++) {
			if ( i > 50 && i< 60) System.arraycopy(Storable.putFloat(50 * .1F), 0, bytes, i * 4, 4);
			else System.arraycopy(Storable.putFloat(i*.1F), 0, bytes, i * 4, 4);
		}
		System.out.println( getLessThanEuqalTo(bytes, 4.9F ) );
	}
	
	public static final byte[] toBytes(final List<Float> inputs) {
		if ( null == inputs) return null;
		byte[] inputsB = new byte[inputs.size() * 4];
		int index = 0;
		for (Float aVal : inputs) {
			System.arraycopy(Storable.putFloat(aVal), 0, inputsB, index * 4, 4);
		}
		return inputsB;
	}
	
	public static final int getEqualToIndex(final byte[] intB, final float matchingNoB) {
		if ( null == intB ) return -1;
		int intBT = intB.length / 4;
		int pos = 0;
		float parsedFloat = -1;
		for (int i = 0; i < intBT; i++) {
			pos = i*4;
			parsedFloat = Storable.getFloat(pos, intB);
			if ( parsedFloat != matchingNoB) continue;
			return i;
		}
		return -1;
	}
	
	public static final void getEqualToIndexes(final byte[] intB, final float matchingNoB, final List<Integer> indexes) {
		if ( null == intB) return;
		
		int intBT = intB.length / 4;
		int pos = 0;
		float parsedFloat = -1;
		for (int i = 0; i < intBT; i++) {
			pos = i*4;
			parsedFloat = Storable.getFloat(pos, intB);
			
			if ( parsedFloat !=  matchingNoB) continue;
			indexes.add(i);
		}
	}	
	
	public static final List<Integer> getGreaterThanIndexes(final byte[] intB, final float matchingNoB) {
		return computeGTGTEQIndexes(intB, matchingNoB, false);
	}	
	
	public static final List<Integer> getGreaterThanEuqalTo(final byte[] intB, final float matchingNoB) {
		return computeGTGTEQIndexes(intB, matchingNoB, true);
	}	
	
	private static final List<Integer> computeGTGTEQIndexes(final byte[] intB, final float matchingNoB, final boolean isEqualCheck) {
		if ( null == intB ) return null;

		int intBT = intB.length / 4;
		int pos = 0;
		List<Integer> indexes = new ArrayList<Integer>();
		float parsedFloat = -1;
		for (int i = 0; i < intBT; i++) {
			pos = i*4;
			parsedFloat = Storable.getFloat(pos, intB);
			if ( isEqualCheck ) {
				if ( parsedFloat < matchingNoB) continue; //Allow when equal
			} else {
				if ( parsedFloat <=  matchingNoB ) continue;
			}
			indexes.add(i);
		}
		return indexes;
	}
	
	public static final List<Integer> getLessThan(final byte[] intB, final float matchingNoB) {
		return checkLTLTEQ(intB, matchingNoB, false);
	}	
	
	public static final List<Integer> getLessThanEuqalTo(final byte[] intB, final float matchingNoB) {
		return checkLTLTEQ(intB, matchingNoB, true);
	}	
	
	private static final List<Integer> checkLTLTEQ(final byte[] intB, final float matchingNoB, final boolean isEqualCheck) {
		if ( null == intB) return null;
		int intBT = intB.length / 4;
		int pos = 0;
		List<Integer> indexes = new ArrayList<Integer>();
		float parsedFloat = -1;
		
		for (int i = 0; i < intBT; i++) {
			pos = i*4;
			parsedFloat = Storable.getFloat(pos, intB);
			
			if ( isEqualCheck ) {
				if ( parsedFloat > matchingNoB) continue; //Allow when equal
			} else {
				if ( parsedFloat >=  matchingNoB ) continue;
			}
			
			indexes.add(i);
		}
		return indexes;
	}
	
	public static final List<Integer> getRange(final byte[] intB, final  float matchingNoStart, final float matchingNoEnd) {
		return computeRangeIndexes(intB, matchingNoStart, matchingNoEnd, false);
	}	
	
	public static final List<Integer> getRangeInclusive(final byte[] intB, final float matchingNoStart, final  float matchingNoEnd) {
		return computeRangeIndexes(intB, matchingNoStart, matchingNoEnd, true);
	}	
	
	private static final List<Integer> computeRangeIndexes(final byte[] intB, final float matchingNoStart, final float matchingNoEnd, final boolean isEqualCheck) {
		
		if ( null == intB ) return null;

		int intBT = intB.length / 4;
		int pos = 0;
		List<Integer> indexes = new ArrayList<Integer>();
		float parsedFloat = -1;
		for (int i = 0; i < intBT; i++) {
			pos = i*4;
			parsedFloat = Storable.getFloat(pos, intB);
			if ( isEqualCheck ) { //Allow when equal
				if ( parsedFloat < matchingNoStart) continue; 
				if ( parsedFloat > matchingNoEnd) continue;
			} else {
				if ( parsedFloat <=  matchingNoStart ) continue;
				if ( parsedFloat >= matchingNoEnd) continue;
			}
			indexes.add(i);
		}
		return indexes;
	}	
}
