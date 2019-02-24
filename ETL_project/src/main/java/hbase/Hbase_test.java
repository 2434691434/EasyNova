/**
 * Hbase_test.java
 * hbase
 * @author   任超                      
*/

package hbase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;

/**
 * 
 * <p>
 * TODO( 连接hbase操作，有建表、插入、查找等)
 * @author   任超                     
 * @Date	 2018年10月16日 	 
 */
public class Hbase_test {
	
	public static Configuration conf = HBaseConfiguration.create();
	public static TableName tablename = TableName.valueOf("testTable");
	
	public static void main(String[] args) {
//		createTable();
//		dropTable();
		
		
//		putTable();
		putsTable();
		
//		get_table();
//		scan_table();
		
//		delete_row();
//		delete_qualifier();
//		delete_columnfimally();
		
		
	}
	
	
	private static void delete_columnfimally() {
		
		Connection conn = null;
		HBaseAdmin admin = null;
		
		try {
			conn  = ConnectionFactory.createConnection(conf);
			admin = (HBaseAdmin) conn.getAdmin();
			
			admin.deleteColumn(tablename, "cfking".getBytes());
			
			
			
		} catch (IOException e) {
			
			// TODO Auto-generated catch block
			e.printStackTrace();
			
		}finally {
			release(conn, admin, null);
		}
		
		
	}


	private static void delete_qualifier() {
		
		
		Connection conn =null;
		HTable table = null;
		try {
			conn  = ConnectionFactory.createConnection(conf);
			table = (HTable) conn.getTable(tablename);
			
			Delete del = new Delete("id_03".getBytes());
			
			del.addColumn("cf_1".getBytes(), "age".getBytes());
			
			
			table.delete(del);
			
			
		} catch (IOException e) {
			
			// TODO Auto-generated catch block
			e.printStackTrace();
			
		}finally {
			release(conn, null, table);
		}
		
		
		
	}


	private static void delete_row() {
		
		Connection conn =null;
		HTable table = null;
		try {
			conn  = ConnectionFactory.createConnection(conf);
			table = (HTable) conn.getTable(tablename);
			
			Delete del = new Delete("id_02".getBytes());
			
			table.delete(del);
			
			
		} catch (IOException e) {
			
			// TODO Auto-generated catch block
			e.printStackTrace();
			
		}finally {
			release(conn, null, table);
		}
		
		
	}


	private static void scan_table() {
		
		Connection conn = null;
		HTable table = null;
		
		try {
			conn = ConnectionFactory.createConnection(conf);
			
			table = (HTable) conn.getTable(tablename);
			
			Scan scan = new Scan();
			scan.setStartRow("id_02".getBytes());
			
			scan.setStopRow("id_04".getBytes());
			
			scan.addFamily("cf_1".getBytes());
			
			scan.setMaxVersions(3);
			
			ResultScanner scanner = table.getScanner(scan);
			for (Result result : scanner) {
				print_result(result);
			}
			
			
		} catch (IOException e) {
			
			// TODO Auto-generated catch block
			e.printStackTrace();
			
		}finally {
			release(conn, null, table);
		}
		
		
	}


	/**
	 * 
	 * TODO(得到一行的数据)
	 * <p>
	 * TODO(这里描述这个方法详情– 可选)  TODO(这里描述每个参数,如果有返回值描述返回值,如果有异常描述异常)
	 */
	private static void get_table() {
		
		Connection conn =null;
		HTable table =null;
		
		try {
			conn = ConnectionFactory.createConnection(conf);
			table = (HTable) conn.getTable(tablename);
			
			Get get =new Get("id_02".getBytes());
			Result res = table.get(get);
			System.out.println(res.toString());
			print_result(res);
			
		} catch (IOException e) {
			e.printStackTrace();
			
		}
		finally {
			release(conn, null, table);
		}
		
	}
	
	
	public static void print_result(Result res){
		
		Cell[] cells = res.rawCells();
		StringBuilder sb = new StringBuilder();
		for (Cell cell : cells) {
			sb.append(new String(CellUtil.cloneRow(cell))).append("\t")
			.append(new String(CellUtil.cloneFamily(cell))).append("\t")
			.append(new String(CellUtil.cloneQualifier(cell))).append("\t")
			.append(new String(CellUtil.cloneValue(cell))).append("\n");
		}
		System.out.println(sb.toString());
	}

	private static void putsTable() {
		
		Connection conn =null;
		HTable table =null;
		
		try {
			conn = ConnectionFactory.createConnection(conf);
			
			table = (HTable) conn.getTable(tablename);
			
			List<Put> puts = new ArrayList<>();
			
			Put put2 = new Put("id_02".getBytes());
			put2.addColumn("cf".getBytes(), "age".getBytes(), "15".getBytes());
			
			Put put3 = new Put("id_02".getBytes());
			put3.addColumn("cf".getBytes(), "name".getBytes(), "name5".getBytes());
			
			Put put4 = new Put("id_03".getBytes());
			put4.addColumn("cf".getBytes(), "age".getBytes(), "16".getBytes());
			
			Put put5 = new Put("id_03".getBytes());
			put5.addColumn("cf".getBytes(), "name".getBytes(), "name3".getBytes());
			
			Put put6 = new Put("id_04".getBytes());
			put6.addColumn("cf".getBytes(), "name".getBytes(), "name4".getBytes());
			
			Put put7 = new Put("id_04".getBytes());
			put7.addColumn("cf".getBytes(), "age".getBytes(), "16".getBytes());
			
			puts.add(put2);
			puts.add(put3);
			puts.add(put4);
			
			puts.add(put5);
			puts.add(put6);
			puts.add(put7);
			
			table.put(puts);
			
		} catch (IOException e) {
			
			e.printStackTrace();
			
		}finally {
			release(conn, null, table);
		}
		
		
	}

	private static void putTable() {
		
		Connection conn =null;
		HTable table =null;
		
		try {
			conn = ConnectionFactory.createConnection(conf);
			table = (HTable) conn.getTable(tablename);
			Put put = new Put(("id_01").getBytes());
			put.addColumn("cf_1".getBytes(), "name".getBytes(), "name1".getBytes());
			
			table.put(put);
			
		} catch (IOException e) {
			
			// TODO Auto-generated catch block
			e.printStackTrace();
			
		}finally {
			release(conn, null, table);
		}
		
		
	}

	private static void dropTable() {
		
		Connection conn = null;
		HBaseAdmin admin =null;
		
		 try {
			conn = ConnectionFactory.createConnection(conf);
			admin = (HBaseAdmin) conn.getAdmin();
			
			admin.disableTable(tablename);
			admin.deleteTable(tablename);
			
		} catch (IOException e) {
			
			e.printStackTrace();
			
		}finally {
			release(conn, admin, null);
		}
		
		
	}

	private static void createTable() {
		
		
		Connection conn = null;
		HBaseAdmin admin = null;
		
		try {
			conn = ConnectionFactory.createConnection(conf);
			admin = (HBaseAdmin) conn.getAdmin();
			
			HTableDescriptor table = new HTableDescriptor(tablename);
			HColumnDescriptor cf_1 = new HColumnDescriptor("cf_1");
			HColumnDescriptor cf_2 = new HColumnDescriptor("cf_2");
			
			table .addFamily(cf_1);
			table.addFamily(cf_2);
			
			if(admin.tableExists(tablename)){
				System.out.println(tablename.toString()+"is exists");
				return;
			}
			admin.createTable(table);
			
		} catch (IOException e) {
			
			e.printStackTrace();
			
		}finally {
			release(conn, admin,null);
		}
		
	}

	private static void release(Connection conn, HBaseAdmin admin ,HTable table) {
		if(conn!=null){
			try {
				conn.close();
			} catch (IOException e) {
				
				e.printStackTrace();
				
			}
		}
		if(admin!=null){
			try {
				admin.close();
			} catch (IOException e) {
				e.printStackTrace();
				
			}
		}
		if(table!=null){
			try {
				table.close();
			} catch (IOException e) {
				e.printStackTrace();
				
			}
		}
	}
	
	
	
}

