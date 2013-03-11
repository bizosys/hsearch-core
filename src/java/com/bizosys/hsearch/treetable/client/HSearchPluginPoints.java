package com.bizosys.hsearch.treetable.client;

import java.io.IOException;

public class HSearchPluginPoints {
	
	public static final int PLUGIN_CALLBACK_ID = 0;
	public static final int PLUGIN_CALLBACK_IDVAL = 1;
	public static final int PLUGIN_CALLBACK_VAL = 2;
	public static final int PLUGIN_CALLBACK_COLS = 3;
	
	public static final int OUTPUT_ID = 0;
	public static final int OUTPUT_IDVAL = 1;
	public static final int OUTPUT_VAL = 2;
	public static final int OUTPUT_COLS = 3;

	public static final int OUTPUT_COUNT = 4;
	public static final int OUTPUT_MIN = 5;
	public static final int OUTPUT_MAX = 6;
	public static final int OUTPUT_AVG = 7;
	public static final int OUTPUT_SUM = 8;
	
	public static final int OUTPUT_MIN_MAX = 9;
	public static final int OUTPUT_MIN_MAX_AVG = 10;
	public static final int OUTPUT_MIN_MAX_COUNT = 11;
	public static final int OUTPUT_MIN_MAX_AVG_COUNT = 12;

	public static final int OUTPUT_MIN_MAX_SUM = 13;
	public static final int OUTPUT_MIN_MAX_SUM_AVG = 14;
	public static final int OUTPUT_MIN_MAX_SUM_COUNT = 15;
	public static final int OUTPUT_MIN_MAX_AVG_SUM_COUNT = 16;
	
	public static final int OUTPUT_FACETS = 17;
	
	private static String EMPTY = "";

	private int callbackCode = PLUGIN_CALLBACK_ID;
	private int outputCode = PLUGIN_CALLBACK_ID;
	private String processingHint = EMPTY;
	
	public HSearchPluginPoints() {
		
	}

	public HSearchPluginPoints(int callbackCode, int outputCode) {
		this.callbackCode = callbackCode;
		this.outputCode = outputCode;
	}

	public HSearchPluginPoints(int callbackCode, int outputCode, String processingHint) {
		this.callbackCode = callbackCode;
		this.outputCode = outputCode;
		this.processingHint = processingHint;
	}

	public HSearchPluginPoints(String code) throws IOException {
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

	public int getOutputType() {
		return this.outputCode;
	}
	
	public int getCallbackType() {
		return this.callbackCode;
	}
	
	public String getProcessingHint() {
		return this.processingHint;
	}
	
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
	
	public String toStringHumanReadable() {
		return ( new Integer(callbackCode).toString() + '|' + new  Integer(outputCode).toString() );
	}

	public static void main(String[] args) throws IOException {
		HSearchPluginPoints o = new HSearchPluginPoints(PLUGIN_CALLBACK_COLS, OUTPUT_MIN_MAX_AVG_SUM_COUNT, " ");
		HSearchPluginPoints x = new HSearchPluginPoints(o.toString());
		System.out.println( x.getProcessingHint());
	}

}
