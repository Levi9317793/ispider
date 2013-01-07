package Test;
//package load;
//
//import java.sql.Connection;
//import java.sql.DriverManager;
//
//public class MysqlLoadTest {
//
//	public static void main(String[] args){
//		Connection[] conn =new Connection(100);
//		for(int i=0;i<300;i++){
//			String driver = "com.mysql.jdbc.Driver";
//			String url = "jdbc:mysql://192.168.20.21:3306/mysql2Hbase";
//			String user = "root";
//			try {
//	
//				Class.forName(driver);
//				if(conn == null)
//					conn = DriverManager.getConnection(url, user,null);
//					conn.setAutoCommit(false);
//					prestRate = conn.prepareStatement(sqlRate);
//					prestPlan = conn.prepareStatement(sqlPlan);
//			}
//			catch(Exception e){
//				e.printStackTrace();
//			}
//		}
//	}
//}
