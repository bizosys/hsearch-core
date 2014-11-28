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

import java.util.Collection;
import java.util.List;

public final class SortedBytesDouble extends SortedBytesBase<Double> { 


	public final static ISortedByte<Double> getInstance() {
		return new SortedBytesDouble();
	}
	
	public final static SortedBytesDouble getInstanceDouble() {
		return new SortedBytesDouble();
	}
	
	private SortedBytesDouble() {
		this.dataSize = 8;
	}	

	
	@Override
	public final Double getValueAt(final int pos) {
		return Storable.getDouble( this.offset + (pos * this.dataSize) , this.inputBytes);
	}	
	
	public final byte[] toBytes(final List<double[]> sortedList, int sortedListAT) {

		byte[] inputsB = new byte[sortedListAT * dataSize];
		
		int index = 0;
		for (double[] aValA : sortedList) {
			for ( double aVal : aValA ) {
				System.arraycopy(Storable.putLong(Double.doubleToLongBits(aVal)), 0, inputsB, index * dataSize, dataSize);
				index++;
				if ( index >= sortedListAT) break;
			}
		}
		return inputsB;
	}
	
	@Override
	public final byte[] toBytes(final Collection<Double> sortedList) {

		int sortedListAT = sortedList.size();
		byte[] inputsB = new byte[sortedListAT * dataSize];
		
		int index = 0;
		for (Double aVal : sortedList) {
			System.arraycopy(Storable.putLong(Double.doubleToLongBits(aVal)), 0, inputsB, index * dataSize, dataSize);
			index++;
		}
		return inputsB;
	}
	
	@Override
	protected final int compare(final byte[] inputB, final int offset, final Double matchNo) {
		double val = Double.longBitsToDouble(Storable.getLong(offset, inputB) );
		if (val == matchNo) return 0;
		if (val > matchNo) return 1;
		return -1;
	}

}
