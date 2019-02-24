
package etl_project;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import javax.sql.DataSource;

import com.mchange.v2.c3p0.ComboPooledDataSource;



public class Test_dataDBpool {
	
	public static final DataSource ds=new ComboPooledDataSource();
		public static void main(String[] args) {
			System.out.println(ds);
			
			Connection conn=null;
			try {
				conn = ds.getConnection();
				System.out.println(conn);
				String str = "select *from renchao08_etl";
				Statement statement = conn.createStatement();
				ResultSet resultSet = statement.executeQuery(str);
				while(resultSet.next()){
					
					System.out.println(resultSet.getInt(4));
				}
				
				
			} catch (SQLException e) {
				e.printStackTrace();
			}finally {
				if(conn!=null){
					try {
						conn.close();
					} catch (SQLException e) {
						e.printStackTrace();
					}
				}
			}
		}
}
*/