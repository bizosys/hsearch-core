import java.util.ArrayList;
import java.util.List;

import junit.framework.TestCase;
import junit.framework.TestFerrari;

import com.bizosys.hsearch.byteutils.ByteStringTest;
import com.bizosys.hsearch.byteutils.SortedBytesArrayTest;
import com.bizosys.hsearch.byteutils.SortedBytesFloatTest;
import com.bizosys.hsearch.byteutils.SortedBytesIntegerTest;
import com.bizosys.hsearch.byteutils.SortedBytesLongCompressedTest;
import com.bizosys.hsearch.byteutils.SortedBytesUnsignedShortTest;
import com.bizosys.hsearch.treetable.Cell2Test;
import com.bizosys.hsearch.treetable.Cell3Test;
import com.bizosys.hsearch.treetable.Cell6Test;


public class TestAll {
	public static void main(String[] args) throws Exception {
		List<TestCase>  testcases = new ArrayList<TestCase>();
		testcases.add(new ByteStringTest());
		testcases.add( new SortedBytesArrayTest() );
		testcases.add( new SortedBytesFloatTest() );
		testcases.add( new SortedBytesIntegerTest() );
		testcases.add( new SortedBytesLongCompressedTest() );
		testcases.add( new SortedBytesUnsignedShortTest() );
		testcases.add( new Cell2Test() );
		testcases.add( new Cell3Test() );
		testcases.add( new Cell6Test() );
		
		for (TestCase t : testcases) {
			TestFerrari.testRandom(t);
			TestFerrari.testMemory(t);
			TestFerrari.testMaxMin(t);
			TestFerrari.testResponse(t);
		}
	}
}
