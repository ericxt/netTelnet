package DBDriverUtil;

import java.sql.DriverManager;
import java.sql.SQLException;

import com.mysql.jdbc.Connection;

public class MysqlDBUtil {
    private static String driver = "com.mysql.jdbc.Driver";
//    private static String url = "jdbc:mysql://127.0.0.1:33306/txie";
    private static String url = "jdbc:mysql://121.199.41.209:3306/xcube?rewriteBatchedStatements=true";
    private static String user = "root"; 
//    private static String password = "289589"; // for localhost
    private static String password = "lab502";
    
    public static Connection getConnection() {
    	try {
			Class.forName(driver);
			Connection conn = null;
			try {
				conn = (Connection) DriverManager.getConnection(url, user, password);
				if(!conn.isClosed()) {					
					System.out.println("Succeeded connecting to the Database!");
					return conn;
				} else
					return null;
			} catch (SQLException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		} catch (ClassNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
    	return null;
    }
    
}
