package com.bizosys.hsearch.kv.impl;

import java.io.IOException;
import java.io.OutputStream;
import java.util.HashMap;
import java.util.Map;

import com.bizosys.hsearch.byteutils.SortedBytesArray;
import com.bizosys.hsearch.byteutils.SortedBytesBoolean;
import com.bizosys.hsearch.byteutils.SortedBytesChar;
import com.bizosys.hsearch.byteutils.SortedBytesDouble;
import com.bizosys.hsearch.byteutils.SortedBytesFloat;
import com.bizosys.hsearch.byteutils.SortedBytesInteger;
import com.bizosys.hsearch.byteutils.SortedBytesLong;
import com.bizosys.hsearch.byteutils.SortedBytesShort;
import com.bizosys.hsearch.byteutils.SortedBytesString;
import com.bizosys.hsearch.treetable.BytesSection;
import com.bizosys.hsearch.treetable.Cell2;
import com.bizosys.hsearch.treetable.Cell2Visitor;

public class ComputeKV implements ICompute {
	
	public static class MergeVisitor<K1, V> implements Cell2Visitor<K1, V> {
		public Cell2<K1, V> mergeCell = null;;
		@Override
		public void visit(K1 k, V v) {
			mergeCell.add(k, v);
		}
	}
	
	public static class RowVisitor<K1, V> implements Cell2Visitor<K1, V> {
		public Map<K1, Object> container = null;
		public int fieldSeq = 0;
		public int totalFields = 0;
		
		@Override
		public void visit(K1 k, V v) {
			container.put(k, v);
		}
	}	
	
	public int kvType = 1;
	public int fieldSeq = 0;
	public int totalFields = 0;

	Cell2<Integer, Boolean> kv_boolean = null;
	Cell2<Integer, Byte> kv_byte = null;
	Cell2<Integer, Short> kv_short = null;
	Cell2<Integer, Integer> kv_integer = null;
	Cell2<Integer, Float> kv_float = null;
	Cell2<Integer, Long> kv_long = null;
	Cell2<Integer, Double> kv_double = null;
	Cell2<Integer, String> kv_string = null;
	
	public Map<Integer, Object> rowContainer = null;
	
	public ComputeKV() {
		
	}
	
	@Override
	public void setCallBackType(final int callbackType) {
		this.kvType = callbackType;
	}

	public void put(final int key, final Object value) {
		switch (this.kvType) {
			case 0:
				if ( null == kv_boolean) kv_boolean = new Cell2<Integer, Boolean>(
					SortedBytesInteger.getInstance(), SortedBytesBoolean.getInstance());
				kv_boolean.add( key, (Boolean) value);
				break;
			case 1:
				if ( null == kv_byte) kv_byte = new Cell2<Integer, Byte>(
					SortedBytesInteger.getInstance(), SortedBytesChar.getInstance());
				kv_byte.add( key, (Byte) value);
				break;
			case 2:
				if ( null == kv_short) kv_short = new Cell2<Integer, Short>(
					SortedBytesInteger.getInstance(), SortedBytesShort.getInstance());
				kv_short.add( key, (Short) value);
				break;
			case 3:
				if ( null == kv_integer) kv_integer = new Cell2<Integer, Integer>(
					SortedBytesInteger.getInstance(), SortedBytesInteger.getInstance());
				kv_integer.add( key, (Integer) value);
				break;
			case 4:
				if ( null == kv_float) kv_float = new Cell2<Integer, Float>(
					SortedBytesInteger.getInstance(), SortedBytesFloat.getInstance());
				kv_float.add( key, (Float) value);
				break;
			case 5:
				if ( null == kv_long) kv_long = new Cell2<Integer, Long>(
					SortedBytesInteger.getInstance(), SortedBytesLong.getInstance());
				kv_long.add( key, (Long) value);
				break;
			case 6:
				if ( null == kv_double) kv_double = new Cell2<Integer, Double>(
					SortedBytesInteger.getInstance(), SortedBytesDouble.getInstance());
				kv_double.add( key, (Double) value);
				break;
			case 7:
				if ( null == kv_string) kv_string = new Cell2<Integer, String>(
					SortedBytesInteger.getInstance(), SortedBytesString.getInstance());
				kv_string.add( key, (String) value);
				break;
		}
	}
	
	public void merge(final ICompute part)  {
		ComputeKV partCasted = (ComputeKV) part;
		
		try {
			switch (this.kvType) {
			case 0:
			{
				if ( null == kv_boolean) kv_boolean = new Cell2<Integer, Boolean>(
						SortedBytesInteger.getInstance(), SortedBytesBoolean.getInstance());
				
				MergeVisitor<Integer, Boolean> visitor = new MergeVisitor<Integer, Boolean>();
				visitor.mergeCell = kv_boolean;
				partCasted.kv_boolean.data = new BytesSection(partCasted.kv_boolean.toBytesOnSortedData()); 
				partCasted.kv_boolean.process(visitor);
				break;
			}
			case 1:
			{	if ( null == kv_byte) kv_byte = new Cell2<Integer, Byte>(
						SortedBytesInteger.getInstance(), SortedBytesChar.getInstance());
				
				MergeVisitor<Integer, Byte> visitor = new MergeVisitor<Integer, Byte>();
				visitor.mergeCell = kv_byte;
				partCasted.kv_byte.data = new BytesSection(partCasted.kv_byte.toBytesOnSortedData()); 
				partCasted.kv_byte.process(visitor);
				break;
			}
			case 2:
			{
				if ( null == kv_short) kv_short = new Cell2<Integer, Short>(
						SortedBytesInteger.getInstance(), SortedBytesShort.getInstance());
				
				MergeVisitor<Integer, Short> visitor = new MergeVisitor<Integer, Short>();
				visitor.mergeCell = kv_short;
				partCasted.kv_short.data = new BytesSection(partCasted.kv_short.toBytesOnSortedData()); 
				partCasted.kv_short.process(visitor);
				break;
			}
			case 3:
			{
				if ( null == kv_integer) kv_integer = new Cell2<Integer, Integer>(
						SortedBytesInteger.getInstance(), SortedBytesInteger.getInstance());
				
				MergeVisitor<Integer, Integer> visitor = new MergeVisitor<Integer, Integer>();
				visitor.mergeCell = kv_integer;
				partCasted.kv_integer.data = new BytesSection(partCasted.kv_integer.toBytesOnSortedData()); 
				partCasted.kv_integer.process(visitor);
				break;
			}
			case 4:
			{
				if ( null == kv_float) kv_float = new Cell2<Integer, Float>(
						SortedBytesInteger.getInstance(), SortedBytesFloat.getInstance());
				
				MergeVisitor<Integer, Float> visitor = new MergeVisitor<Integer, Float>();
				visitor.mergeCell = kv_float;
				partCasted.kv_float.data = new BytesSection(partCasted.kv_float.toBytesOnSortedData()); 
				partCasted.kv_float.process(visitor);
				break;
			}
			case 5:
			{
				if ( null == kv_long) kv_long = new Cell2<Integer, Long>(
						SortedBytesInteger.getInstance(), SortedBytesLong.getInstance());
				
				MergeVisitor<Integer, Long> visitor = new MergeVisitor<Integer, Long>();
				visitor.mergeCell = kv_long;
				partCasted.kv_long.data = new BytesSection(partCasted.kv_long.toBytesOnSortedData()); 
				partCasted.kv_long.process(visitor);
				break;
			}
			case 6:
			{
				if ( null == kv_double) kv_double = new Cell2<Integer, Double>(
						SortedBytesInteger.getInstance(), SortedBytesDouble.getInstance());
				
				MergeVisitor<Integer, Double> visitor = new MergeVisitor<Integer, Double>();
				visitor.mergeCell = kv_double;
				partCasted.kv_double.data = new BytesSection(partCasted.kv_double.toBytesOnSortedData()); 
				partCasted.kv_double.process(visitor);
				break;
			}
			case 7:
			{
				if ( null == kv_string) kv_string = new Cell2<Integer, String>(
						SortedBytesInteger.getInstance(), SortedBytesString.getInstance());
				
				MergeVisitor<Integer, String> visitor = new MergeVisitor<Integer, String>();
				visitor.mergeCell = kv_string;
				partCasted.kv_string.data = new BytesSection(partCasted.kv_string.toBytesOnSortedData()); 
				partCasted.kv_string.process(visitor);
				break;
			}
		}
	} catch (IOException ex) {
			throw new NullPointerException(ex.getMessage());
	 }
	}
	
	public ComputeKV createNew() {
		ComputeKV kv = new ComputeKV();
		kv.kvType = this.kvType;
		return kv;
	}
	
	public void clear() {
		switch (this.kvType) {
		case 0:
			kv_boolean.sortedList.clear();
			kv_boolean.data = null;
			break;
		case 1:
			kv_byte.sortedList.clear();
			kv_byte.data = null;
			break;
		case 2:
			kv_short.sortedList.clear();
			kv_short.data = null;
			break;
		case 3:
			kv_integer.sortedList.clear();
			kv_integer.data = null;
			break;
		case 4:
			kv_float.sortedList.clear();
			kv_float.data = null;
			break;
		case 5:
			kv_long.sortedList.clear();
			kv_long.data = null;
			break;
		case 6:
			kv_double.sortedList.clear();
			kv_double.data = null;
			break;
		case 7:
			kv_string.sortedList.clear();
			kv_string.data = null;
			break;
		}
	}	
	
	public byte[] toBytes() throws IOException {
		byte[] data = null; 
		switch (this.kvType) {
			case 0:
				data = kv_boolean.toBytesOnSortedData();
				break;
			case 1:
				data = kv_byte.toBytesOnSortedData();
				break;
			case 2:
				data = kv_short.toBytesOnSortedData();
				break;
			case 3:
				data = kv_integer.toBytesOnSortedData();
				break;
			case 4:
				data = kv_float.toBytesOnSortedData();
				break;
			case 5:
				data = kv_long.toBytesOnSortedData();
				break;
			case 6:
				data = kv_double.toBytesOnSortedData();
				break;
			case 7:
				data = kv_string.toBytesOnSortedData();
				break;

			default: break;
		}
		return data;
	}
	
	@Override
	public void put(final byte[] data) throws IOException {

		for (byte[] dataChunk : SortedBytesArray.getInstanceArr().parse(data).values()) {
			switch (this.kvType) {
				case 0:
				{
					kv_boolean = new Cell2<Integer, Boolean>(
							SortedBytesInteger.getInstance(), SortedBytesBoolean.getInstance(), dataChunk);
					
					RowVisitor<Integer, Boolean> visitor = new RowVisitor<Integer, Boolean>();
					visitor.fieldSeq = this.fieldSeq;
					visitor.totalFields = this.totalFields;
					visitor.container = rowContainer;
					kv_boolean.process(visitor);
					break;
				}
				case 1:
				{
					kv_byte = new Cell2<Integer, Byte>(
							SortedBytesInteger.getInstance(), SortedBytesChar.getInstance(), dataChunk);
					
					RowVisitor<Integer, Byte> visitor = new RowVisitor<Integer, Byte>();
					visitor.fieldSeq = this.fieldSeq;
					visitor.totalFields = this.totalFields;
					visitor.container = rowContainer;
					kv_byte.process(visitor);
					break;
				}
				case 2:
				{
					kv_short = new Cell2<Integer, Short>(
							SortedBytesInteger.getInstance(), SortedBytesShort.getInstance(), dataChunk);
					
					RowVisitor<Integer, Short> visitor = new RowVisitor<Integer, Short>();
					visitor.fieldSeq = this.fieldSeq;
					visitor.totalFields = this.totalFields;
					visitor.container = rowContainer;
					kv_short.process(visitor);
					break;
				}
				case 3:
				{
					kv_integer = new Cell2<Integer, Integer>(
							SortedBytesInteger.getInstance(), SortedBytesInteger.getInstance(), dataChunk);
					
					RowVisitor<Integer, Integer> visitor = new RowVisitor<Integer, Integer>();
					visitor.fieldSeq = this.fieldSeq;
					visitor.totalFields = this.totalFields;
					visitor.container = rowContainer;
					kv_integer.process(visitor);
					break;
				}
				case 4:
				{
					kv_float = new Cell2<Integer, Float>(
							SortedBytesInteger.getInstance(), SortedBytesFloat.getInstance(), dataChunk);
					
					RowVisitor<Integer, Float> visitor = new RowVisitor<Integer, Float>();
					visitor.fieldSeq = this.fieldSeq;
					visitor.totalFields = this.totalFields;
					visitor.container = rowContainer;
					kv_float.process(visitor);
					break;
				}
				case 5:
				{
					kv_long = new Cell2<Integer, Long>(
							SortedBytesInteger.getInstance(), SortedBytesLong.getInstance(), dataChunk);
					
					RowVisitor<Integer, Long> visitor = new RowVisitor<Integer, Long>();
					visitor.fieldSeq = this.fieldSeq;
					visitor.totalFields = this.totalFields;
					visitor.container = rowContainer;
					kv_long.process(visitor);
					break;
				}
				case 6:
				{
					kv_double = new Cell2<Integer, Double>(
							SortedBytesInteger.getInstance(), SortedBytesDouble.getInstance(), dataChunk);
					
					RowVisitor<Integer, Double> visitor = new RowVisitor<Integer, Double>();
					visitor.fieldSeq = this.fieldSeq;
					visitor.totalFields = this.totalFields;
					visitor.container = rowContainer;
					kv_double.process(visitor);
					break;
				}
				case 7:
				{
					kv_string = new Cell2<Integer, String>(
							SortedBytesInteger.getInstance(), SortedBytesString.getInstance(), dataChunk);
					
					RowVisitor<Integer, String> visitor = new RowVisitor<Integer, String>();
					visitor.fieldSeq = this.fieldSeq;
					visitor.totalFields = this.totalFields;
					visitor.container = rowContainer;
					kv_string.process(visitor);
					break;
				}
				default: break;
			}
		}
	}

	@Override
	public void setStreamWriter(OutputStream out) {
	}
	
	@Override
	public void onComplete() {
		
	}
}
