package com.bizosys.hsearch.treetable.client;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.util.StringUtils;
import java.text.ParseException;

import com.bizosys.hsearch.functions.FunctionRepos;
import com.bizosys.hsearch.util.LineReaderUtil;

public class HSearchProcessingInstruction {
	
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
	private FunctionExpr[] functionCalls = null;
	
	public HSearchProcessingInstruction() {
		
	}

	public HSearchProcessingInstruction(int callbackCode, int outputCode ) {
		this.callbackCode = callbackCode;
		this.outputCode = outputCode;
	}

	public HSearchProcessingInstruction(int callbackCode, int outputCode, FunctionExpr[] functionCalls) {
		this.callbackCode = callbackCode;
		this.outputCode = outputCode;
		this.functionCalls = functionCalls;
	}

	public HSearchProcessingInstruction(String code) throws ParseException {
		int fDivider = code.indexOf('|');
		this.callbackCode = new Integer(code.substring(0, fDivider));
		
		int sDivider = code.indexOf('|', fDivider + 1);
		if ( sDivider < 0 ) {
			this.outputCode = new Integer(code.substring(fDivider + 1));
		} else {
			this.outputCode = new Integer(code.substring(fDivider + 1, sDivider));
			this.functionCalls = FunctionExpr.deser(code.substring(sDivider + 1));
		}
	}

	public int getOutputType() {
		return this.outputCode;
	}
	
	public int getCallbackType() {
		return this.callbackCode;
	}
	
	public FunctionExpr[] getFunctionCalls() {
		return this.functionCalls;
	}
	
	public String toString() {
		StringBuilder sb = new StringBuilder(32);
		sb.append(callbackCode);
		sb.append('|');
		sb.append(outputCode);
		if ( null != this.functionCalls ) {
			sb.append('|');
			sb.append(FunctionExpr.ser(this.functionCalls));
		}		
		return ( sb.toString());
	}
	
	public static final class FunctionExpr {
		public int cellNo;
		public int functionCode;
		
		public FunctionExpr(int cellNo, int functionCode) {
			this.cellNo = cellNo;
			this.functionCode = functionCode;
		}
		
		public static FunctionExpr[] deser(String text) throws ParseException {
			List<String> cellNoAndFunc = new ArrayList<String>();
			LineReaderUtil.fastSplit(cellNoAndFunc, text, ',');
			FunctionExpr[] funcs = new FunctionExpr[cellNoAndFunc.size()];
			
			int seq = 0;
			for (String cellNoAndFuncStr : cellNoAndFunc) {
				int fDivider = cellNoAndFuncStr.indexOf(':');
				if ( fDivider < 0 ) {
					throw new ParseException("Improper function serilization: " + cellNoAndFuncStr, 0);
				} 
				
				funcs[seq] = new FunctionExpr(
					new Integer(cellNoAndFuncStr.substring(0, fDivider)),
					new Integer(cellNoAndFuncStr.substring(fDivider + 1))
					);
				seq++;
			}
			return funcs;
		}
		
		public static String ser(FunctionExpr[] functions) {
			StringBuilder sb = null;
			
			for (FunctionExpr functionExpr : functions) {
				if ( null == sb) {
					sb = new StringBuilder();
				} else {
					sb.append(',');
				}
				sb.append(functionExpr.cellNo);
				sb.append(':');
				sb.append(functionExpr.functionCode);
			}
			return sb.toString();
		}
	}
		

	public static void main(String[] args) throws ParseException {
		FunctionExpr count = new FunctionExpr(1, FunctionRepos.COUNT);
		FunctionExpr min = new FunctionExpr(21, FunctionRepos.COUNT);
		HSearchProcessingInstruction o = 
			new HSearchProcessingInstruction(PLUGIN_CALLBACK_COLS, OUTPUT_COLS, 
				new FunctionExpr[]{count, min});
		
		HSearchProcessingInstruction x = new HSearchProcessingInstruction(o.toString());
		System.out.println( x.getCallbackType());
		System.out.println( x.getOutputType());
		for (FunctionExpr func : x.getFunctionCalls()) {
			System.out.println( func.cellNo);
			System.out.println( func.functionCode);			
		}
	}
	
	
	

}
