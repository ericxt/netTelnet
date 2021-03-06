package dBDriverUtil;

import java.sql.DriverManager;
import java.sql.SQLException;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.mysql.jdbc.Connection;

public class MysqlDBUtil {
	static Logger logger = LogManager.getLogger(MysqlDBUtil.class.getName());
	private static String driver = "com.mysql.jdbc.Driver";

	private static String url = "jdbc:mysql://localhost:3306/xcube?rewriteBatchedStatements=true";

	// rongdata remote mysql(small one)
	// private static String url =
	// "jdbc:mysql://121.199.41.209:3306/xcube?rewriteBatchedStatements=true";

	// rongdata remote mysql(big one)
	// private static String url =
	// "jdbc:mysql://121.40.82.174:3306/xcube?rewriteBatchedStatements=true";

	// gmax remote mysql
	// private static String url =
	// "jdbc:mysql://120.55.117.63:3306/xcube?rewriteBatchedStatements=true";
	private static String user = "root";
//	private static String password = "289589"; // for localhost

	// rongdata remote mysql
	 private static String password = "lab502";

	// gmax remote mysql
	// private static String password = "gmaxdb";

	public static Connection getConnection() {
		try {
			Class.forName(driver);
			Connection conn = null;
			try {
				conn = (Connection) DriverManager.getConnection(url, user,
						password);
				if (!conn.isClosed()) {
					System.out.println("Succeeded connecting to the Database!");
					logger.info("Succeeded connecting to the Database!");
					return conn;
				} else
					return null;
			} catch (SQLException e) {
				// TODO Auto-generated catch block
				logger.catching(e);
				e.printStackTrace();
			}
		} catch (ClassNotFoundException e) {
			// TODO Auto-generated catch block
			logger.catching(e);
			e.printStackTrace();
		}
		return null;
	}

}
