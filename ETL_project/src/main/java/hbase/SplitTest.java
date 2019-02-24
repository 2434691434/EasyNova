package hbase;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.hadoop.hbase.util.RegionSplitter.SplitAlgorithm;



/**
 * 
 * 读region拆分文件
 *
 */
public class SplitTest implements SplitAlgorithm {
	
	public static List<String> getSplitDataList(){
		
		ArrayList<String> list= new ArrayList<String>();
		InputStream is = SplitTest.class.getResourceAsStream("/split_format");
		BufferedReader br = null;
		try {
		br=	new BufferedReader(new InputStreamReader(is, "utf-8"));
		String line = null;
		while ((line=br.readLine())!=null) {
			list.add(line);
		}
		} catch (UnsupportedEncodingException e) {
			// TODO 自动生成的 catch 块
			e.printStackTrace();
		} catch (IOException e) {
			// TODO 自动生成的 catch 块
			e.printStackTrace();
		}
		return list;
		
	}
	
	
	@Override
	public byte[] split(byte[] start, byte[] end) {
		// TODO 自动生成的方法存根
		return null;
	}

	@Override
	public byte[][] split(int numRegions) {

		List<String> list = getSplitDataList();
		ArrayList<byte[]> region = new ArrayList<byte []>();
		
		for (String s : list) {
			region.add(s.getBytes());
		}
		
		byte[][] b = new byte[0][];
		return region.toArray(b);
	}

	@Override
	public byte[] firstRow() {
		// TODO 自动生成的方法存根
		return null;
	}

	@Override
	public byte[] lastRow() {
		// TODO 自动生成的方法存根
		return null;
	}

	@Override
	public void setFirstRow(String userInput) {
		// TODO 自动生成的方法存根

	}

	@Override
	public void setLastRow(String userInput) {
		// TODO 自动生成的方法存根

	}

	@Override
	public byte[] strToRow(String input) {
		// TODO 自动生成的方法存根
		return null;
	}

	@Override
	public String rowToStr(byte[] row) {
		// TODO 自动生成的方法存根
		return null;
	}

	@Override
	public String separator() {
		// TODO 自动生成的方法存根
		return null;
	}

	@Override
	public void setFirstRow(byte[] userInput) {
		// TODO 自动生成的方法存根

	}

	@Override
	public void setLastRow(byte[] userInput) {
		// TODO 自动生成的方法存根

	}
	
	/*public static void main(String[] args) {
		List<String> splitDataList = getSplitDataList();
		System.out.println(Arrays.toString(splitDataList.toArray()));
	}*/

}
