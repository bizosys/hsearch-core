package com.bizosys.hsearch.functions;

import java.text.ParseException;
import java.util.ArrayList;
import java.util.List;

import com.bizosys.hsearch.util.LineReaderUtil;

public class FunctionExpr {
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
