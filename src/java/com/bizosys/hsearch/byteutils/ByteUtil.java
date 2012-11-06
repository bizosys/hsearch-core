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
import java.io.UnsupportedEncodingException;
//import java.util.Map;

import com.google.protobuf.ByteString;

public class ByteUtil {
	/**
	 * Compare byte values
	 * @param inputOffset	Starting position of compare with Byte Array
	 * @param inputBytes	Compare with Bytes
	 * @param compareBytes	Compare to Bytes
	 * @return	True if matches
	 */
	public static boolean compareBytes(byte[] inputBytes, int inputOffset, byte[] compareBytes) {

		int inputBytesT = inputBytes.length;
		int compareBytesT = compareBytes.length;
		if ( compareBytesT !=  inputBytesT - inputOffset) return false;
		
		if ( compareBytes[0] != inputBytes[inputOffset]) return false;
		if ( compareBytes[compareBytesT - 1] != inputBytes[compareBytesT + inputOffset - 1] ) return false;
		
		switch (compareBytesT)
		{
			case 3:
				return compareBytes[1] == inputBytes[1 + inputOffset];
			case 4:
				return compareBytes[1] == inputBytes[1 + inputOffset] && 
					compareBytes[2] == inputBytes[2 + inputOffset];
			case 5:
				return compareBytes[1] == inputBytes[1+ inputOffset] && 
					compareBytes[2] == inputBytes[2+ inputOffset] && 
					compareBytes[3] == inputBytes[3+ inputOffset];
			case 6:
				return compareBytes[1] == inputBytes[1+ inputOffset] && 
				compareBytes[3] == inputBytes[3+ inputOffset] && 
				compareBytes[2] == inputBytes[2+ inputOffset] && 
				compareBytes[4] == inputBytes[4+ inputOffset];
			case 7:
			case 8:
			case 9:
			case 10:
			case 11:
			case 12:
			case 13:
			case 14:
			case 15:
			case 16:
			case 17:
			case 18:
			case 19:
			case 20:
			case 21:
			case 22:
			case 23:
			case 24:
			case 25:
			case 26:
			case 27:
			case 28:
			case 29:
			case 30:
				for ( int i=inputOffset; i< compareBytesT - 1; i++) {
					if ( compareBytes[i] != inputBytes[inputOffset + i]) return false;
				}
				break;
				
			case 31:
				
				for ( int a = 1; a <= 6; a++) {
					if ( ! 
					(compareBytes[a] == inputBytes[a+inputOffset] && 
					compareBytes[a+6] == inputBytes[a+6+inputOffset] && 
					compareBytes[a+12] == inputBytes[a+12+inputOffset] && 
					compareBytes[a+18] == inputBytes[a+18+inputOffset] && 
					compareBytes[a+24] == inputBytes[a+24+inputOffset]) ) return false;
				}
				break;
			default:

				for ( int i=inputOffset; i< compareBytesT - 1; i++) {
					if ( compareBytes[i] != inputBytes[inputOffset + i]) return false;
				}
		}
		return true;
	}
	
	public static boolean compareBytes(byte[] inputBytes, int offset, int length, byte[] compareBytes) throws IOException {
		int requiredLen = (offset + length);
		if ( inputBytes.length < requiredLen ) throw new IOException("Corrupted input bytes");
		
		int compLen = compareBytes.length;
		
		if ( compLen != length) return false;
		
		int readPos = offset;
		for ( int i=0; i<compLen; i++) {
			if ( inputBytes[readPos] != compareBytes[i]) return false;
			readPos++;
		}
		return true;
	}

	/**
	 *	Compare two bytes 
	 * @param inputBytes	Compare with Bytes
	 * @param compareBytes	Compare to Bytes
	 * @return	True if matches
	 */
	public static boolean compareBytes(byte[] inputBytes, byte[] compareBytes) {
		return compareBytes(inputBytes,0,compareBytes);
	}
	
	
	/**
	 *	Compare two characters
	 * @param inputBytes	Compare with character array
	 * @param compareBytes	Compare to character array
	 * @return	True if matches
	 */
	public static boolean compareBytes(char[] inputBytes, char[] compareBytes) {

		int inputBytesT = inputBytes.length;
		int compareBytesT = compareBytes.length;
		if ( compareBytesT !=  inputBytesT) return false;
		
		if ( compareBytes[0] != inputBytes[0]) return false;
		if ( compareBytes[compareBytesT - 1] != inputBytes[compareBytesT - 1] ) return false;
		
		switch (compareBytesT)
		{
			case 3:
				return compareBytes[1] == inputBytes[1];
			case 4:
				return compareBytes[1] == inputBytes[1] && 
					compareBytes[2] == inputBytes[2];
			case 5:
				return compareBytes[1] == inputBytes[1] && 
					compareBytes[2] == inputBytes[2] && 
					compareBytes[3] == inputBytes[3];
			case 6:
				return compareBytes[1] == inputBytes[1] && 
				compareBytes[3] == inputBytes[3] && 
				compareBytes[2] == inputBytes[2] && 
				compareBytes[4] == inputBytes[4];
			default:
				compareBytesT--;
				for ( int i=0; i< compareBytesT; i++) {
					if ( compareBytes[i] != inputBytes[i]) return false;
				}
		}
		return true;
	}
	
	/**
	 * Form a short value reading 2 bytes
	 * @param offset	Bytes read start position
	 * @param inputBytes	Input Bytes
	 * @return	Short representation
	 */
	public static short toShort(byte[] inputBytes, int offset) {
		return (short) (
			(inputBytes[offset] << 8 ) + ( inputBytes[++offset] & 0xff ) );
	}
	
	/**
	 * Forms a byte array from a Short data
	 * @param value	Short data
	 * @return	2 bytes
	 */
	public static byte[] toBytes( short value ) {

		return new byte[] { 
			(byte)(value >> 8 & 0xff), 
			(byte)(value & 0xff) };
	}
	
	/**
	 * Form a integer value reading 4 bytes
	 * @param offset	Bytes read start position
	 * @param inputBytes	Input Bytes
	 * @return	Integer representation
	 */
	public static int toInt(byte[] inputBytes, int offset) {
		
		int intVal = (inputBytes[offset] << 24 ) + 
		( (inputBytes[++offset] & 0xff ) << 16 ) + 
		(  ( inputBytes[++offset] & 0xff ) << 8 ) + 
		( inputBytes[++offset] & 0xff );
		return intVal;
	}
	
	/**
	 * Forms a byte array from a Integer data
	 * @param value	Integer data
	 * @return	4 bytes
	 */
	public static byte[] toBytes( int value ) {
		return new byte[] { 
			(byte)(value >> 24), 
			(byte)(value >> 16 ), 
			(byte)(value >> 8 ), 
			(byte)(value) }; 
	}
	
	public static float toFloat(byte[] inputBytes, int offset) {
		
		return Float.intBitsToFloat(toInt(inputBytes, offset));
	}
	
	public static byte[] toBytes( float value ) {
		return (toBytes(Float.floatToIntBits(value)));
	}
	
	
	/**
	 * Form a Long value reading 8 bytes
	 * @param offset	Bytes read start position
	 * @param inputBytes	Input Bytes
	 * @return	Long representation
	 */
	public static long toLong(final byte[] inputBytes, int offset) {
		
		if ( 0 == inputBytes.length) return 0;
		
		long longVal = ( ( (long) (inputBytes[offset]) )  << 56 )  + 
		( (inputBytes[++offset] & 0xffL ) << 48 ) + 
		( (inputBytes[++offset] & 0xffL ) << 40 ) + 
		( (inputBytes[++offset] & 0xffL ) << 32 ) + 
		( (inputBytes[++offset] & 0xffL ) << 24 ) + 
		( (inputBytes[++offset] & 0xff ) << 16 ) + 
		( (inputBytes[++offset] & 0xff ) << 8 ) + 
		( inputBytes[++offset] & 0xff );
		return longVal;
	}
	
	/**
	 * Forms a byte array from a long data
	 * @param value	Long data
	 * @return	8 bytes
	 */
	public static byte[] toBytes(long value) {
		return new byte[]{
			(byte)(value >> 56), 
			(byte)(value >> 48 ), 
			(byte)(value >> 40 ), 
			(byte)(value >> 32 ), 
			(byte)(value >> 24 ), 
			(byte)(value >> 16 ), 
			(byte)(value >> 8 ), 
			(byte)(value ) };		
	}
	
	public static double toDouble(final byte[] inputBytes, int offset) {
		return Double.longBitsToDouble(toLong(inputBytes, offset));
	}
	
	
	public static byte[] toBytes(double value) {
		return toBytes(Double.doubleToLongBits(value));
	}
	
	/**
	 * Parse a byte array to form a UTF-8 String
	 * @param inputBytes	Input bytes array
	 * @return	A UTF-8 String
	 */
	public static String toString(byte[] inputBytes, int offset, int length) {
		try {
			return new String( inputBytes , offset, length, "UTF-8");
		} catch (UnsupportedEncodingException ex) {
			return new String(inputBytes);
		}
	}	
	
	/**
	 * Form a String value format UTF-8
	 * @param inputObj	Input String
	 * @return	bytes representation
	 */
	public static byte[] toBytes( String inputObj) {
		try {
			return inputObj.getBytes("UTF-8");
		} catch (UnsupportedEncodingException ex) {
			return inputObj.getBytes();
		}
		
	}
	
/**	
	public static ByteString toBytesLongFloatMap(Map<Long, Float> input) {
		
		ByteArrays.ArrayLong.Builder keyBuilder = ByteArrays.ArrayLong.newBuilder();
		ByteArrays.ArrayFloat.Builder valBuilder = ByteArrays.ArrayFloat.newBuilder();
		
		for (Long mapKey : input.keySet()) keyBuilder.addVal(mapKey);
		for (Float mapVal : input.values()) valBuilder.addVal(mapVal);
		return joinKV(keyBuilder.build().toByteString(), valBuilder.build().toByteString());
	}
	
	public static ByteString toBytesLongDoubleMap(Map<Long, Double> input) {
		
		ByteArrays.ArrayLong.Builder keyBuilder = ByteArrays.ArrayLong.newBuilder();
		ByteArrays.ArrayDouble.Builder valBuilder = ByteArrays.ArrayDouble.newBuilder();
		
		for (long mapKey : input.keySet()) keyBuilder.addVal(mapKey);
		for (double mapVal : input.values()) valBuilder.addVal(mapVal);
		return joinKV(keyBuilder.build().toByteString(), valBuilder.build().toByteString());
	}	
	
	public static ByteString toBytesLongIntMap(Map<Long, Integer> input) {
		
		ByteArrays.ArrayLong.Builder keyBuilder = ByteArrays.ArrayLong.newBuilder();
		ByteArrays.ArrayInt.Builder valBuilder = ByteArrays.ArrayInt.newBuilder();
		
		for (long mapKey : input.keySet()) keyBuilder.addVal(mapKey);
		for (int mapVal : input.values()) valBuilder.addVal(mapVal);
		return joinKV(keyBuilder.build().toByteString(), valBuilder.build().toByteString());
	}
	
	public static ByteString toBytesLongBooleanMap(Map<Long, Boolean> input) {
		
		ByteArrays.ArrayLong.Builder keyBuilder = ByteArrays.ArrayLong.newBuilder();
		ByteArrays.ArrayBool.Builder valBuilder = ByteArrays.ArrayBool.newBuilder();
		
		for (long mapKey : input.keySet()) keyBuilder.addVal(mapKey);
		for (boolean mapVal : input.values()) valBuilder.addVal(mapVal);
		return joinKV(keyBuilder.build().toByteString(), valBuilder.build().toByteString());
	}	
	
	public static ByteString toBytesLongBytesMap(Map<Long, byte[]> input) {
		
		ByteArrays.ArrayLong.Builder keyBuilder = ByteArrays.ArrayLong.newBuilder();
		ByteArrays.ArrayBytes.Builder valBuilder = ByteArrays.ArrayBytes.newBuilder();
		
		for (long mapKey : input.keySet()) keyBuilder.addVal(mapKey);
		for (byte[] mapVal : input.values()) valBuilder.addVal(ByteString.copyFrom(mapVal));

		return joinKV(keyBuilder.build().toByteString(), valBuilder.build().toByteString());
	}
	
	public static ByteString toBytesLongBytesStringMap(Map<Long, ByteString> input) {
		
		ByteArrays.ArrayLong.Builder keyBuilder = ByteArrays.ArrayLong.newBuilder();
		ByteArrays.ArrayBytes.Builder valBuilder = ByteArrays.ArrayBytes.newBuilder();
		
		for (long mapKey : input.keySet()) keyBuilder.addVal(mapKey);
		for (ByteString mapVal : input.values()) valBuilder.addVal(mapVal);

		return joinKV(keyBuilder.build().toByteString(), valBuilder.build().toByteString());
	}
	
///////
	public static ByteString toBytesIntegerFloatMap(Map<Integer, Float> input) {
		
		ByteArrays.ArrayInt.Builder keyBuilder = ByteArrays.ArrayInt.newBuilder();
		ByteArrays.ArrayFloat.Builder valBuilder = ByteArrays.ArrayFloat.newBuilder();
		
		for (Integer mapKey : input.keySet()) keyBuilder.addVal(mapKey);
		for (Float mapVal : input.values()) valBuilder.addVal(mapVal);
		return joinKV(keyBuilder.build().toByteString(), valBuilder.build().toByteString());
	}
	
	public static ByteString toBytesIntegerDoubleMap(Map<Integer, Double> input) {
		
		ByteArrays.ArrayInt.Builder keyBuilder = ByteArrays.ArrayInt.newBuilder();
		ByteArrays.ArrayDouble.Builder valBuilder = ByteArrays.ArrayDouble.newBuilder();
		
		for (int mapKey : input.keySet()) keyBuilder.addVal(mapKey);
		for (double mapVal : input.values()) valBuilder.addVal(mapVal);
		return joinKV(keyBuilder.build().toByteString(), valBuilder.build().toByteString());
	}	
	
	public static ByteString toBytesIntegerIntMap(Map<Integer, Integer> input) {
		
		ByteArrays.ArrayInt.Builder keyBuilder = ByteArrays.ArrayInt.newBuilder();
		ByteArrays.ArrayInt.Builder valBuilder = ByteArrays.ArrayInt.newBuilder();
		
		for (int mapKey : input.keySet()) keyBuilder.addVal(mapKey);
		for (int mapVal : input.values()) valBuilder.addVal(mapVal);
		return joinKV(keyBuilder.build().toByteString(), valBuilder.build().toByteString());
	}
	
	public static ByteString toBytesIntegerBooleanMap(Map<Integer, Boolean> input) {
		
		ByteArrays.ArrayInt.Builder keyBuilder = ByteArrays.ArrayInt.newBuilder();
		ByteArrays.ArrayBool.Builder valBuilder = ByteArrays.ArrayBool.newBuilder();
		
		for (int mapKey : input.keySet()) keyBuilder.addVal(mapKey);
		for (boolean mapVal : input.values()) valBuilder.addVal(mapVal);
		return joinKV(keyBuilder.build().toByteString(), valBuilder.build().toByteString());
	}	
	
	public static ByteString toBytesIntegerBytesMap(Map<Integer, byte[]> input) {
		
		ByteArrays.ArrayInt.Builder keyBuilder = ByteArrays.ArrayInt.newBuilder();
		ByteArrays.ArrayBytes.Builder valBuilder = ByteArrays.ArrayBytes.newBuilder();
		
		for (int mapKey : input.keySet()) keyBuilder.addVal(mapKey);
		for (byte[] mapVal : input.values()) valBuilder.addVal(ByteString.copyFrom(mapVal));

		return joinKV(keyBuilder.build().toByteString(), valBuilder.build().toByteString());
	}
	
	public static ByteString toBytesIntegerBytesStringMap(Map<Integer, ByteString> input) {
		
		ByteArrays.ArrayInt.Builder keyBuilder = ByteArrays.ArrayInt.newBuilder();
		ByteArrays.ArrayBytes.Builder valBuilder = ByteArrays.ArrayBytes.newBuilder();
		
		for (int mapKey : input.keySet()) keyBuilder.addVal(mapKey);
		for (ByteString mapVal : input.values()) valBuilder.addVal(mapVal);

		return joinKV(keyBuilder.build().toByteString(), valBuilder.build().toByteString());
	}	
//////
	
	public static PartialParsed<Integer> parseIntegerMap(byte[] data) throws IOException {

		ByteArrays.ArrayBytes kvBytes = ByteArrays.ArrayBytes.parseFrom(data);
		ByteString keyData = kvBytes.getValList().get(0);
		ByteArrays.ArrayInt pks = ByteArrays.ArrayInt.parseFrom(keyData);
		
		return new PartialParsed<Integer>(pks.getValList(), kvBytes.getValList().get(1)); 
	}	
	
	public static PartialParsed<Integer> parseIntegerMap(ByteString data) throws IOException {

		ByteArrays.ArrayBytes kvBytes = ByteArrays.ArrayBytes.parseFrom(data);
		ByteString keyData = kvBytes.getValList().get(0);
		ByteArrays.ArrayInt pks = ByteArrays.ArrayInt.parseFrom(keyData);
		
		return new PartialParsed<Integer>(pks.getValList(), kvBytes.getValList().get(1)); 
	}		
	

	public static PartialParsed<Long> parseLongMap(byte[] data) throws IOException {

		ByteArrays.ArrayBytes kvBytes = ByteArrays.ArrayBytes.parseFrom(data);
		ByteString keyData = kvBytes.getValList().get(0);
		ByteArrays.ArrayLong pks = ByteArrays.ArrayLong.parseFrom(keyData);
		
		return new PartialParsed<Long>(pks.getValList(), kvBytes.getValList().get(1)); 
	}	
	
	public static PartialParsed<Long> parseLongMap(ByteString data) throws IOException {

		ByteArrays.ArrayBytes kvBytes = ByteArrays.ArrayBytes.parseFrom(data);
		ByteString keyData = kvBytes.getValList().get(0);
		ByteArrays.ArrayLong pks = ByteArrays.ArrayLong.parseFrom(keyData);
		
		return new PartialParsed<Long>(pks.getValList(), kvBytes.getValList().get(1)); 
	}	
*/	
	
	
	/**
	 * Put two array on one Array
	 * @param k
	 * @param v
	 * @return
	 */
	public static ByteString joinKV(ByteString k, ByteString v) {

		ByteArrays.ArrayBytes.Builder kvBuilder = ByteArrays.ArrayBytes.newBuilder();
		kvBuilder.addVal(k);
		kvBuilder.addVal(v);
		return kvBuilder.build().toByteString();		
	}
	
	public static ByteString joinKV(byte[] k, ByteString v) {

		ByteArrays.ArrayBytes.Builder kvBuilder = ByteArrays.ArrayBytes.newBuilder();
		kvBuilder.addVal(ByteString.copyFrom(k));
		kvBuilder.addVal(v);
		return kvBuilder.build().toByteString();		
	}	
	
	public static ByteString joinKV(byte[] k, byte[] v) {

		ByteArrays.ArrayBytes.Builder kvBuilder = ByteArrays.ArrayBytes.newBuilder();
		kvBuilder.addVal(ByteString.copyFrom(k));
		kvBuilder.addVal(ByteString.copyFrom(v));
		return kvBuilder.build().toByteString();		
	}		
	
	public static KVBytes getKV(byte[] data) throws IOException {
		
		ByteArrays.ArrayBytes kvBytes = ByteArrays.ArrayBytes.parseFrom(data);
		KVBytes kv = new KVBytes();
		kv.keyB = kvBytes.getValList().get(0);		
		kv.valueB = kvBytes.getValList().get(1);		
		return kv;
	}
	
	public static KVBytes getKV(ByteString data) throws IOException {
		
		ByteArrays.ArrayBytes kvBytes = ByteArrays.ArrayBytes.parseFrom(data);
		KVBytes kv = new KVBytes();
		kv.keyB = kvBytes.getValList().get(0);		
		kv.valueB = kvBytes.getValList().get(1);		
		return kv;
	}	
	
	
	
    /**
     * Convert a byte to a 8 bits
     * @param b	A byte
     * @return	8 bits
     */
	public static final boolean[] toBits(byte b) {
        boolean[] bits = new boolean[8];
        for (int i = 0; i < bits.length; i++) {
            bits[i] = ((b & (1 << i)) != 0);
        }
        return bits;
    }

	/**
	 * Convert 8 bits to a Byte
	 * @param bits	Bits array. Reading happens from position 0
	 * @return	1 Byte
	 */
    public static byte fromBits(boolean[] bits) {
		return fromBits(bits, 0);
    }
	
    /**
     * Converting 8 Bits to a Byte
     * @param bits	array of bits
     * @param offset	Read starting position
     * @return	1 Byte
     */
	public static byte fromBits(boolean[] bits, int offset) {
		int value = 0;
        for (int i = 0; i < 8; i++) {
			if(bits[i] == true) {
				value = value | (1 << i);
			}
        }
        return (byte)value;
	}	
}
