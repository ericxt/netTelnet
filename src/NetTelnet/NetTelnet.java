package NetTelnet;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.PrintStream;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Calendar;
import java.util.Date;
import java.util.Timer;
import java.util.TimerTask;

import org.apache.commons.net.telnet.TelnetClient;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import DBDriverUtil.MysqlDBUtil;
import MysqlDAOImpl.MysqlDAOImpl;

import com.mysql.jdbc.Connection;



public class NetTelnet {
	static Logger logger = LogManager.getLogger(NetTelnet.class.getName());

	public TelnetClient telnet = new TelnetClient();
	public InputStream in;
	public PrintStream out;
	public static MysqlDAOImpl daoImpl = new MysqlDAOImpl();

	public NetTelnet(String ip, int port, String user, String password) {
		try {
			telnet.connect(ip, port);
			logger.info("telnet >>> ip : " + ip + ", port : " + port);
			in = telnet.getInputStream();
			out = new PrintStream(telnet.getOutputStream());
			// 根据root用户设置结束符
			// this.prompt = user.equals("root") ? '#' : '$';
			// login(user, password);
		} catch (Exception e) {
			logger.catching(e);
			e.printStackTrace();
		}
	}

	/**
	 * login telnet with userName and password
	 * 
	 * @param user
	 * @param password
	 */
	public void login(String user, String password) {
		readUtil("login:");
		write(user);
		readUtil("Password:");
		write(password);
	}

	/**
	 * naive read method
	 * 
	 * @param command
	 */
	public void readUtil(String command) {
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

	/**
	 * improved readAndWrite method receive the raw data and insert the data
	 * into database
	 * 
	 * @param command
	 * @throws SQLException
	 */
	public void readAndWrite(String command) throws SQLException {
		int count = 0;

		// for market_quotation
		String sql = "insert ignore into market_quotation(TradingTime,ContractId,ExchangeId,PreSettlementPrice,"
				+ "CurrSettlementPrice,AveragePrice,PreClosePrice,CurrClosePrice,CurrOpenPrice,PreHoldings,"
				+ "Holdings,LatestPrice,Volume,TurnOver,TopQuotation,BottomQuotation,TopPrice,BottomPrice,"
				+ "PreDelta,CurrDelta,BidPrice1,AskPrice1,BidVolume1,AskVolume1,BidPrice2,AskPrice2,BidVolume2,"
				+ "AskVolume2,BidPrice3,AskPrice3,BidVolume3,AskVolume3,BidPrice4,AskPrice4,BidVolume4,AskVolume4,"
				+ "BidPrice5,AskPrice5,BidVolume5,AskVolume5) values(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,"
				+ "?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)";
//		String sql = "insert ignore into xcube.market_quotation(TradingTime,ContractId,ExchangeId,PreSettlementPrice,CurrSettlementPrice,AveragePrice,"
//				+ "PreClosePrice,CurrClosePrice,CurrOpenPrice,PreHoldings,Holdings,LatestPrice,Volume,"
//				+ "TurnOver,TopQuotation,BottomQuotation,TopPrice,BottomPrice,PreDelta,CurrDelta,"
//				+ "BidPrice1,AskPrice1,BidVolume1,AskVolume1,BidPrice2,AskPrice2,BidVolume2,AskVolume2,"
//				+ "BidPrice3,AskPrice3,BidVolume3,AskVolume3,BidPrice4,AskPrice4,BidVolume4,AskVolume4,"
//				+ "BidPrice5,AskPrice5,BidVolume5,AskVolume5) "
//				+ "values(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?);";
		Connection conn = MysqlDBUtil.getConnection();
		logger.info("insert raw data from telnet to database >>> " + sql);
		PreparedStatement prepareStatement = conn.prepareStatement(sql);
		InputStreamReader inputStreamReader = new InputStreamReader(in);
		BufferedReader bufferedReader = new BufferedReader(inputStreamReader);

		try {
			conn.setAutoCommit(false);

			while (bufferedReader.readLine() != null) {
				String record = bufferedReader.readLine().toString().trim();
				// remove the beginning prefix "QUOTE" and the commas
				record = record.substring(6);
				count++;
				String[] split = record.split(",");
				// daoImpl.insertForTA(split, prepareStatement); // for TA
				daoImpl.insertForQUOTE(split, prepareStatement);
				prepareStatement.addBatch();

				if (count % 500 == 0) {
					prepareStatement.executeBatch();
					conn.commit();
					// prepareStatement.close();
					// conn.close();
					// conn = MysqlDBUtil.getConnection();
					// conn.setAutoCommit(false);
					// prepareStatement = conn.prepareStatement(sql);
					System.out.println("insert 500 records");
				}
//				System.out.println(count);
				Calendar calendar = Calendar.getInstance();
				System.out.println("current time >>> " + calendar.getTime()
						+ " ,count >>> " + count);

				if (isNoonExpired(calendar)) {
					System.out.println("date is expired, sleep!");
					if (count % 500 != 0) {
						prepareStatement.executeBatch();
						conn.commit();
						System.out.println("rest records is " + (count % 500));
					}
					try {
						Thread.sleep(90 * 60 * 1000);
					} catch (InterruptedException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					};
				} else if (isAfterExpired(calendar)) {
					System.out.println("date is expired, break!");
					if (count % 500 != 0) {
						prepareStatement.executeBatch();
						conn.commit();
						System.out.println("rest records is " + (count % 500));
					}
					break;
				}
			}

			prepareStatement.close();
			conn.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			logger.catching(e);
			e.printStackTrace();
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			logger.catching(e);
			e.printStackTrace();
		}
	}

	private boolean isAfterExpired(Calendar calendar) {
		// TODO Auto-generated method stub
		if (calendar == null) {
			System.out.println("afternoon calendar is null");
			return false;
		}
		int hour = calendar.get(calendar.HOUR_OF_DAY);
		int minute = calendar.get(calendar.MINUTE);
		if ((hour == 15 && minute > 16) || (hour > 15)) {
			return true;
		}
		return false;
	}

	private boolean isNoonExpired(Calendar calendar) {
		// TODO Auto-generated method stub
		if (calendar == null) {
			System.out.println("noon calendar is null");
			return false;
		}
		int hour = calendar.get(calendar.HOUR_OF_DAY);
		int minute = calendar.get(calendar.MINUTE);
		if ((hour == 11 && minute > 32) || (hour > 11 && hour < 13)) {
			return true;
		}
		return false;
	}

	/**
	 * write to the outputStream
	 * 
	 * @param value
	 */
	public void write(String value) {
		try {
			out.println(value);
			out.flush();
			System.out.println(">>>write");
		} catch (Exception e) {
			logger.catching(e);
			e.printStackTrace();
		}
	}

	/**
	 * send the command
	 * 
	 * @param command
	 */
	public void sendCommand(String command) {
		logger.info("substitute QUOTE via command >>> " + command);
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
			logger.catching(e);
			e.printStackTrace();
		}
	}

	/**
	 * disconnect the telnet connection
	 */
	public void disconnect() {
		try {
			telnet.disconnect();
		} catch (Exception e) {
			logger.catching(e);
			e.printStackTrace();
		}
	}
	
	

	public static void main(String[] args) throws SQLException {
//		try {
//			System.out.println("启动Telnet...");
//			logger.info("启动Telnet...");
//			String ip = "203.187.171.249";
//			int port = 33331;
//			String user = "";
//			String password = "";
//			NetTelnet telnet = new NetTelnet(ip, port, user, password);
//			byte[] bytes = new byte[256];
//			System.out.println(telnet.in.read(bytes));
//			System.out.println(new String(bytes));
//
//			// telnet.sendCommand("STA"); // substitute TA
//			telnet.sendCommand("SQUOTE");
//			telnet.sendCommand("UQUOTE");
//			telnet.sendCommand("QUIT");
//			System.out.println("显示结果");
//			logger.info("raw data extraction ended");
//			telnet.disconnect();
//		} catch (Exception e) {
//			logger.catching(e);
//			e.printStackTrace();
//		}
		
		Calendar calendar = Calendar.getInstance();
		int year = calendar.get(Calendar.YEAR);
		int month = calendar.get(Calendar.MONTH);
		int day = calendar.get(Calendar.DAY_OF_MONTH);
		calendar.set(year, month, day + 1, 9, 15, 0);
		Date date = calendar.getTime();
		System.out.println(date);

		int period = 24 * 60 * 60 * 1000;
		Timer timer = new Timer();
		TimerTask task = new RawDataExtraction();
		timer.schedule(task, date, period);

	}
}

class RawDataExtraction extends TimerTask {
	static Logger logger = LogManager.getLogger();

	public void run() {
		// TODO Auto-generated method stub
		try {
			System.out.println("启动Telnet...");
			logger.info("启动Telnet >>> " + new Date(System.currentTimeMillis()));
			String ip = "203.187.171.249";
			int port = 33331;
			String user = "";
			String password = "";
			NetTelnet telnet = new NetTelnet(ip, port, user, password);
			byte[] bytes = new byte[256];
			System.out.println(telnet.in.read(bytes));
			System.out.println(new String(bytes));

			// telnet.sendCommand("STA"); // substitute TA
			telnet.sendCommand("SQUOTE");
			telnet.sendCommand("UQUOTE");
			telnet.sendCommand("QUIT");
			System.out.println("显示结果");
			logger.info("raw data extraction ended");
			telnet.disconnect();
		} catch (Exception e) {
			logger.catching(e);
			e.printStackTrace();
		}
		
	}
	
}