package com.bizosys.hsearch.functions;

public interface IFunction<T> {
	void eval(boolean input);
	void eval(short input);
	void eval(int input);
	void eval(float input);
	void eval(long input);
	void eval(double input); 
	void eval(String input);
	void eval(byte[] input);
	
	T getValue();
}
