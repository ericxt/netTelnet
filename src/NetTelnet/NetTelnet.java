package NetTelnet;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.PrintStream;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import org.apache.commons.net.telnet.TelnetClient;

import com.mysql.jdbc.Connection;

import DBDriverUtil.MysqlDBUtil;
import MysqlDAOImpl.MysqlDAOImpl;

public class NetTelnet {
	private TelnetClient telnet = new TelnetClient();
	private InputStream in;
	private PrintStream out;
	private static MysqlDAOImpl daoImpl = new MysqlDAOImpl();

	// 普通用户结束
	public NetTelnet(String ip, int port, String user, String password) {
		try {
			telnet.connect(ip, port);
			in = telnet.getInputStream();
			out = new PrintStream(telnet.getOutputStream());
			// 根据root用户设置结束符
			// this.prompt = user.equals("root") ? '#' : '$';
			// login(user, password);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	/** * 登录 * * @param user * @param password */
	public void login(String user, String password) {
		readUntil("login:");
		write(user);
		readUntil("Password:");
		write(password);
	}

	/** * 读取分析结果 * * @param pattern * @return */
	public void readUntil(String command) {
		try {
			System.out.println(">>>readUtil : " + command);
			int count = 0;
			StringBuffer sb = new StringBuffer();
			char ch = (char) in.read();
			while (true) {
				sb.append(ch);
				if (ch == '\n') {
					String record = sb.toString().trim();
					// System.out.println("original>>> " + sb.toString());
					if (record.contains("IF") || record.contains("SH")
							|| record.contains("SZ")) {
						// System.out.println("handled>>>" + record);
						String[] split = record.split(",");
						MysqlDAOImpl mysqlDAOImpl = new MysqlDAOImpl();
						mysqlDAOImpl.insert(split);
						for (int i = 0; i < split.length; i++) {
							// System.out.print(split[i]+split[i].length() +
							// "  ");
						}
						// System.out.println("\n");
						count++;
						System.out.println(count);
					}
					sb.delete(0, sb.length() - 1);
				}
				ch = (char) in.read();
			}
			// System.out.println("sending UTA");
			// sendCommand("UTA");
			// System.out.println("sending QUIT");
			// sendCommand("QUIT");
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	/* 改进的读写操作 */
	public void readAndWrite(String command) throws SQLException {
		int count = 0;

		// for index_ta
		// String sql =
		// "insert into index_ta(Flag,TransactionTime,ContractId,TAIndex,Buy1Price,"
		// +
		// "Buy2Price,Buy3Price,Buy4Price,Buy5Price,Buy1Num,Buy2Num,Buy3Num,Buy4Num,"
		// +
		// "Buy5Num,Sell1Price,Sell2Price,Sell3Price,Sell4Price,Sell5Price,Sell1Num,"
		// +
		// "Sell2Num,Sell3Num,Sell4Num,Sell5Num,m_dZJSJ,m_dJJSJ,m_dCJJJ,m_dZSP,m_dJSP,"
		// +
		// "m_dJKP,m_nZCCL,m_nCCL,m_dZXJ,m_nCJSL,m_dCJJE,m_dZGBJ,m_dZDBJ,m_dZGJ,m_dZDJ,m_dZXSD,m_dJXSD) "
		// +
		// "value(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)";

		// for market_quotation
		String sql = "insert into market_quotation(TradingTime,ContractId,ExchangeId,PreSettlementPrice,"
				+ "CurrSettlementPrice,AveragePrice,PreClosePrice,CurrClosePrice,CurrOpenPrice,PreHoldings,"
				+ "Holdings,LatestPrice,Volume,TurnOver,TopQuotation,BottomQuotation,TopPrice,BottomPrice,"
				+ "PreDelta,CurrDelta,BidPrice1,AskPrice1,BidVolume1,AskVolume1,BidPrice2,AskPrice2,BidVolume2,"
				+ "AskVolume2,BidPrice3,AskPrice3,BidVolume3,AskVolume3,BidPrice4,AskPrice4,BidVolume4,AskVolume4,"
				+ "BidPrice5,AskPrice5,BidVolume5,AskVolume5) value(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,"
				+ "?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)";

		Connection conn = MysqlDBUtil.getConnection();
		PreparedStatement prepareStatement = conn.prepareStatement(sql);
		InputStreamReader inputStreamReader = new InputStreamReader(in);
		BufferedReader bufferedReader = new BufferedReader(inputStreamReader);
		try {
			while (bufferedReader.readLine() != null) {
				conn.setAutoCommit(false);
				String record = bufferedReader.readLine().toString().trim();
				record = record.substring(6);
				// if (record.contains("IF") || record.contains("SH") ||
				// record.contains("SZ")) {
				count++;
				String[] split = record.split(",");
//				daoImpl.insertForTA(split, prepareStatement);    // for TA
				daoImpl.insertForQUOTE(split, prepareStatement);
				prepareStatement.addBatch();
				if (count % 500 == 0) {
					prepareStatement.executeBatch();
					conn.commit();
					prepareStatement.close();
					conn.close();
					conn = MysqlDBUtil.getConnection();
					conn.setAutoCommit(false);
					prepareStatement = conn.prepareStatement(sql);
					System.out.println("insert 500 records");
				}
				System.out.println("count >>> " + count);
				// }
			}
			if (count % 500 != 0) {
				prepareStatement.executeBatch();
				conn.commit();
			}

			prepareStatement.close();
			conn.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	/** * 写操作 * * @param value */
	public void write(String value) {
		try {
			out.println(value);
			out.flush();
			System.out.println(">>>write");
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	/** * 向目标发送命令字符串 * * @param command * @return */
	public void sendCommand(String command) {
		try {
			System.out.println(">>>sendCommand : " + command);
			if (command.startsWith("S")) {
				write(command);
				// readUntil(command); //slow efficiency
				readAndWrite(command);
			} else {
				write(command);
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	/** * 关闭连接 */
	public void disconnect() {
		try {
			telnet.disconnect();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	public static void main(String[] args) throws SQLException {
		// Connection conn = MysqlDBUtil.getConnection();
		// Statement statement = conn.createStatement();
		// String sql = "select * from mysql.user";
		// ResultSet resultSet = statement.executeQuery(sql);
		// while (resultSet.next()) {
		// String host = resultSet.getString("host");
		// String user = resultSet.getString("user");
		// System.out.println("host: " + host + "   " + "user:" + user);
		// }

		try {
			System.out.println("启动Telnet...");
			String ip = "203.187.171.249";
			int port = 33331;
			String user = "";
			String password = "";
			NetTelnet telnet = new NetTelnet(ip, port, user, password);
			byte[] bytes = new byte[256];
			System.out.println(telnet.in.read(bytes));
			System.out.println(new String(bytes));
			
//			telnet.sendCommand("STA");    // substitute TA
			telnet.sendCommand("SQUOTE");

			System.out.println("显示结果");
			telnet.disconnect();
		} catch (Exception e) {
			e.printStackTrace();
		} 

	}
}