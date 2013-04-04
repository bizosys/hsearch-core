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
package com.bizosys.hsearch.treetable.client;

import java.text.ParseException;

public final class HSearchProcessingInstruction {
	
	private static String EMPTY = "";
	
	public static final int PLUGIN_CALLBACK_ID = 0;
	public static final int PLUGIN_CALLBACK_IDVAL = 1;
	public static final int PLUGIN_CALLBACK_VAL = 2;
	public static final int PLUGIN_CALLBACK_COLS = 3;
	
	public static final int OUTPUT_ID = 0;
	public static final int OUTPUT_IDVAL = 1;
	public static final int OUTPUT_VAL = 2;
	public static final int OUTPUT_COLS = 3;
	

	private int callbackCode = PLUGIN_CALLBACK_ID;
	private int outputCode = PLUGIN_CALLBACK_ID;
	private String processingHint = EMPTY;
	
	public HSearchProcessingInstruction() {
		
	}

	public HSearchProcessingInstruction(final int callbackCode, final int outputCode ) {
		this.callbackCode = callbackCode;
		this.outputCode = outputCode;
	}

	public HSearchProcessingInstruction(final int callbackCode, final int outputCode, final String processingHint) {
		this.callbackCode = callbackCode;
		this.outputCode = outputCode;
		this.processingHint = processingHint;
	}

	public HSearchProcessingInstruction(final String code) throws ParseException {
		int fDivider = code.indexOf('|');
		this.callbackCode = new Integer(code.substring(0, fDivider));
		
		int sDivider = code.indexOf('|', fDivider + 1);
		if ( sDivider < 0 ) {
			this.outputCode = new Integer(code.substring(fDivider + 1));
			this.processingHint = EMPTY;
		} else {
			this.outputCode = new Integer(code.substring(fDivider + 1, sDivider));
			this.processingHint = code.substring(sDivider + 1);
		}
	}

	public final int getOutputType() {
		return this.outputCode;
	}
	
	public final int getCallbackType() {
		return this.callbackCode;
	}
	
	public final String getProcessingHint() {
		return this.processingHint;
	}
	
	@Override
	public String toString() {
		StringBuilder sb = new StringBuilder(32);
		sb.append(callbackCode);
		sb.append('|');
		sb.append(outputCode);
		if ( this.processingHint.length() > 0 ) {
			sb.append('|');
			sb.append(processingHint);
		}		
		return ( sb.toString());
	}
	
	public static void main(String[] args) throws ParseException {

		HSearchProcessingInstruction o = 
			new HSearchProcessingInstruction(PLUGIN_CALLBACK_COLS, OUTPUT_COLS, "id1,id2,id3");
		
		HSearchProcessingInstruction x = new HSearchProcessingInstruction(o.toString());
		System.out.println( x.getCallbackType());
		System.out.println( x.getOutputType());
		System.out.println( x.getProcessingHint());
		
	}
	
	
	

}
