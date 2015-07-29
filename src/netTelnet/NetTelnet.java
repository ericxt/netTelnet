package netTelnet;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.PrintStream;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Calendar;
import java.util.Date;

import mysqlDAOImpl.MysqlDAOImpl;

import org.apache.commons.net.telnet.TelnetClient;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.mysql.jdbc.Connection;

import dBDriverUtil.MysqlDBUtil;
import dataExtraction.SettlementDataExtraction;

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
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	/**
	 * improved readAndWrite method receive the raw data and insert the ticker
	 * detail data into database
	 * 
	 * @param command
	 * @throws SQLException
	 */
	public void readAndWriteForTicker(String command) throws SQLException {
		int count = 0;

		// for market_quotation
		String stockSql = "insert ignore into stock_quotation(TradingTime,ContractId,ExchangeId,PreSettlementPrice,"
				+ "CurrSettlementPrice,AveragePrice,PreClosePrice,CurrClosePrice,CurrOpenPrice,PreHoldings,"
				+ "Holdings,LatestPrice,Volume,TurnOver,TopQuotation,BottomQuotation,TopPrice,BottomPrice,"
				+ "PreDelta,CurrDelta,BidPrice1,AskPrice1,BidVolume1,AskVolume1,BidPrice2,AskPrice2,BidVolume2,"
				+ "AskVolume2,BidPrice3,AskPrice3,BidVolume3,AskVolume3,BidPrice4,AskPrice4,BidVolume4,AskVolume4,"
				+ "BidPrice5,AskPrice5,BidVolume5,AskVolume5) values(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,"
				+ "?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)";
		String futureSql = "insert ignore into futures_quotation(TradingTime,ContractId,ExchangeId,PreSettlementPrice,"
				+ "CurrSettlementPrice,AveragePrice,PreClosePrice,CurrClosePrice,CurrOpenPrice,PreHoldings,"
				+ "Holdings,LatestPrice,Volume,TurnOver,TopQuotation,BottomQuotation,TopPrice,BottomPrice,"
				+ "PreDelta,CurrDelta,BidPrice1,AskPrice1,BidVolume1,AskVolume1,BidPrice2,AskPrice2,BidVolume2,"
				+ "AskVolume2,BidPrice3,AskPrice3,BidVolume3,AskVolume3,BidPrice4,AskPrice4,BidVolume4,AskVolume4,"
				+ "BidPrice5,AskPrice5,BidVolume5,AskVolume5) values(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,"
				+ "?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)";
		String debtSql = "insert ignore into debt_quotation(TradingTime,ContractId,ExchangeId,PreSettlementPrice,"
				+ "CurrSettlementPrice,AveragePrice,PreClosePrice,CurrClosePrice,CurrOpenPrice,PreHoldings,"
				+ "Holdings,LatestPrice,Volume,TurnOver,TopQuotation,BottomQuotation,TopPrice,BottomPrice,"
				+ "PreDelta,CurrDelta,BidPrice1,AskPrice1,BidVolume1,AskVolume1,BidPrice2,AskPrice2,BidVolume2,"
				+ "AskVolume2,BidPrice3,AskPrice3,BidVolume3,AskVolume3,BidPrice4,AskPrice4,BidVolume4,AskVolume4,"
				+ "BidPrice5,AskPrice5,BidVolume5,AskVolume5) values(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,"
				+ "?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)";
		String indexSql = "insert ignore into index_quotation(TradingTime,ContractId,ExchangeId,PreSettlementPrice,"
				+ "CurrSettlementPrice,AveragePrice,PreClosePrice,CurrClosePrice,CurrOpenPrice,PreHoldings,"
				+ "Holdings,LatestPrice,Volume,TurnOver,TopQuotation,BottomQuotation,TopPrice,BottomPrice,"
				+ "PreDelta,CurrDelta,BidPrice1,AskPrice1,BidVolume1,AskVolume1,BidPrice2,AskPrice2,BidVolume2,"
				+ "AskVolume2,BidPrice3,AskPrice3,BidVolume3,AskVolume3,BidPrice4,AskPrice4,BidVolume4,AskVolume4,"
				+ "BidPrice5,AskPrice5,BidVolume5,AskVolume5) values(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,"
				+ "?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)";
		Connection conn = MysqlDBUtil.getConnection();
		logger.info("insert futures raw data from telnet to database >>> "
				+ futureSql);
		logger.info("insert debt raw data from telnet to database >>> "
				+ debtSql);
		logger.info("insert index raw data from telnet to database >>> "
				+ indexSql);
		logger.info("insert stock raw data from telnet to database >>> "
				+ stockSql);

		// truncate the table : delete the previous days data
		String stockTruncateSql = "truncate table xcube.stock_quotation;";
		String futuresTruncateSql = "truncate table xcube.futures_quotation;";
		String debtTruncateSql = "truncate table xcube.debt_quotation;";
		String indexTruncateSql = "truncate table xcube.index_quotation;";
		conn.prepareStatement(stockTruncateSql).executeUpdate();
		logger.info("truncate table xcube.stock_quotation");
		conn.prepareStatement(futuresTruncateSql).executeUpdate();
		logger.info("truncate table xcube.futures_quotation");
		conn.prepareStatement(debtTruncateSql).executeUpdate();
		logger.info("truncate table xcube.debt_quotation");
		conn.prepareStatement(indexTruncateSql).executeUpdate();
		logger.info("truncate table xcube.index_quotation");

		PreparedStatement futurePrepareStatement = conn
				.prepareStatement(futureSql);
		PreparedStatement debtPrepareStatement = conn.prepareStatement(debtSql);
		PreparedStatement indexPrepareStatement = conn
				.prepareStatement(indexSql);
		PreparedStatement stockPrepareStatement = conn
				.prepareStatement(stockSql);
		InputStreamReader inputStreamReader = new InputStreamReader(in);
		BufferedReader bufferedReader = new BufferedReader(inputStreamReader);

		try {
			conn.setAutoCommit(false);
			String record = null;

			while ((record = bufferedReader.readLine().toString().trim()) != null) {
				Calendar calendar = Calendar.getInstance();
				// if(isAfterExpired(calendar)) break;
				// if (bufferedReader.readLine().length() < 10) break;
				// record = record.toString().trim();
				System.out.println(record);
				// remove the beginning prefix "QUOTE" and the commas
				record = record.substring(6);
				count++;
				String[] split = record.split(",");
				if (split[1].startsWith("S")) {
					daoImpl.insertForQUOTE(split, stockPrepareStatement);
					stockPrepareStatement.addBatch();
				} else if (split[1].startsWith("TA")
						|| split[1].startsWith("TC")) {
					daoImpl.insertForQUOTE(split, futurePrepareStatement);
					futurePrepareStatement.addBatch();
				} else if (split[1].matches("^(TF|T)[0-9]+")) {
					daoImpl.insertForQUOTE(split, debtPrepareStatement);
					debtPrepareStatement.addBatch();
				} else if (split[1].startsWith("I")) {
					daoImpl.insertForQUOTE(split, indexPrepareStatement);
					indexPrepareStatement.addBatch();
				}

				if (count % 500 == 0) {
					stockPrepareStatement.executeBatch();
					futurePrepareStatement.executeBatch();
					debtPrepareStatement.executeBatch();
					indexPrepareStatement.executeBatch();
					conn.commit();
					System.out.println("insert 500 records");
				}
				System.out.println("current time >>> " + calendar.getTime()
						+ " ,count >>> " + count);

				if (isNoonExpired(calendar)) {
					System.out.println("date is expired, sleep!");
					if (count % 500 != 0) {
						stockPrepareStatement.executeBatch();
						futurePrepareStatement.executeBatch();
						debtPrepareStatement.executeBatch();
						indexPrepareStatement.executeBatch();
						conn.commit();
						System.out.println("rest records is " + (count % 500));
					}
					try {
						long curMillis = calendar.getTimeInMillis();
						calendar.set(calendar.get(Calendar.YEAR),
								calendar.get(Calendar.MONTH),
								calendar.get(Calendar.DAY_OF_MONTH), 13, 0, 0);
						long expectedMillis = calendar.getTimeInMillis();
						System.out
								.println("Now is NoonExpired time, sleeping for "
										+ (expectedMillis - curMillis)
										+ " millis.");
						Thread.sleep(expectedMillis - curMillis);
						// Thread.sleep(90 * 60 * 1000);
					} catch (InterruptedException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
				} else if (isAfterExpired(calendar)) {
					System.out.println("date is expired, break!");
					if (count % 500 != 0) {
						stockPrepareStatement.executeBatch();
						futurePrepareStatement.executeBatch();
						debtPrepareStatement.executeBatch();
						indexPrepareStatement.executeBatch();
						conn.commit();
						System.out.println("rest records is " + (count % 500));
					}
					logger.info("Now is AfterNoon Expired >>> " + new Date(System.currentTimeMillis()));
					break;
				}
			}

			stockPrepareStatement.close();
			logger.info("close stockPrepareStatement");
			futurePrepareStatement.close();
			logger.info("close futurePrepareStatement");
			debtPrepareStatement.close();
			logger.info("close debtPrepareStatement");
			indexPrepareStatement.close();
			logger.info("close indexPrepareStatement");
			conn.close();
			logger.info("close Connection");
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

	/**
	 * extract the trading sentiment data and insert into database
	 * 
	 * @param command
	 * @throws SQLException
	 */
	public void readAndWriteForSentiment(String command) throws SQLException {
		int count = 0;

		String sql = "insert ignore into com_trading_sentiment(TradingTime, Ticker, "
				+ "TradingSentiment) values(?,?,?)";
		Connection conn = MysqlDBUtil.getConnection();
		logger.info("insert trading sentiment data from telnet to database >>> "
				+ sql);
		PreparedStatement prepareStatement = conn.prepareStatement(sql);
		InputStreamReader inputStreamReader = new InputStreamReader(in);
		BufferedReader bufferedReader = new BufferedReader(inputStreamReader);

		try {
			conn.setAutoCommit(false);
			String record = null;
			while ((record = bufferedReader.readLine().toString().trim()) != null) {
				Calendar calendar = Calendar.getInstance();
				System.out.println(record);
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
					System.out.println("insert 500 trading sentiment records");
				}
				// System.out.println(count);
				System.out.println("current time >>> " + calendar.getTime()
						+ " ,count >>> " + count);

				if (isNoonExpired(calendar)) {
					System.out.println("date is expired, sleep!");
					if (count % 500 != 0) {
						prepareStatement.executeBatch();
						conn.commit();
						System.out.println("rest trading sentiment records is "
								+ (count % 500));
					}
					try {
						long curMillis = calendar.getTimeInMillis();
						calendar.set(calendar.get(Calendar.YEAR),
								calendar.get(Calendar.MONTH),
								calendar.get(Calendar.DAY_OF_MONTH), 13, 0, 0);
						long expectedMillis = calendar.getTimeInMillis();
						Thread.sleep(expectedMillis - curMillis);
						// Thread.sleep(90 * 60 * 1000);
					} catch (InterruptedException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
				} else if (isAfterExpired(calendar)) {
					System.out.println("date is expired, break!");
					if (count % 500 != 0) {
						prepareStatement.executeBatch();
						conn.commit();
						System.out.println("rest trading sentiment records is "
								+ (count % 500));
					}
					break;
				}
			}

			prepareStatement.close();
			conn.close();
			
			new SettlementDataExtraction().operate();
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
		int hour = calendar.get(Calendar.HOUR_OF_DAY);
		int minute = calendar.get(Calendar.MINUTE);
		if ((hour == 15 && minute > 17) || (hour > 15)) {
			System.out.println("afterexpired >>> true");
			return true;
		}
		System.out.println("afterexpired >>> false");
		return false;
	}

	private boolean isNoonExpired(Calendar calendar) {
		// TODO Auto-generated method stub
		if (calendar == null) {
			System.out.println("noon calendar is null");
			return false;
		}
		int hour = calendar.get(Calendar.HOUR_OF_DAY);
		int minute = calendar.get(Calendar.MINUTE);
		if ((hour == 11 && minute > 32) || (hour > 11 && hour < 13)) {
			System.out.println("NoonExpired >>> true");
			return true;
		}
		System.out.println("NoonExpired >>> false");
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
				readAndWriteForTicker(command);
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

	/**
	 * public static void main(String[] args) throws SQLException { // try { //
	 * System.out.println("启动Telnet..."); // logger.info("启动Telnet..."); //
	 * String ip = "203.187.171.249"; // int port = 33331; // String user = "";
	 * // String password = ""; // NetTelnet telnet = new NetTelnet(ip, port,
	 * user, password); // byte[] bytes = new byte[256]; //
	 * System.out.println(telnet.in.read(bytes)); // System.out.println(new
	 * String(bytes)); // // // telnet.sendCommand("STA"); // substitute TA //
	 * telnet.sendCommand("SQUOTE"); // telnet.sendCommand("UQUOTE"); //
	 * telnet.sendCommand("QUIT"); // System.out.println("显示结果"); //
	 * logger.info("raw data extraction ended"); // telnet.disconnect(); // }
	 * catch (Exception e) { // logger.catching(e); // e.printStackTrace(); // }
	 * 
	 * Calendar calendar = Calendar.getInstance(); int year =
	 * calendar.get(Calendar.YEAR); int month = calendar.get(Calendar.MONTH);
	 * int day = calendar.get(Calendar.DAY_OF_MONTH); calendar.set(year, month,
	 * day, 9, 15, 0); Date date = calendar.getTime(); System.out.println(date);
	 * 
	 * int period = 24 * 60 * 60 * 1000; Timer timer = new Timer(); TimerTask
	 * task = new RawDataExtraction(); timer.schedule(task, date, period);
	 * 
	 * }
	 **/
}

/**
 * class RawDataExtraction extends TimerTask { static Logger logger =
 * LogManager.getLogger();
 * 
 * public void run() { // TODO Auto-generated method stub try {
 * System.out.println("启动Telnet..."); logger.info("启动Telnet >>> " + new
 * Date(System.currentTimeMillis())); String ip = "203.187.171.249"; int port =
 * 33331; String user = ""; String password = ""; NetTelnet telnet = new
 * NetTelnet(ip, port, user, password); byte[] bytes = new byte[1024]; int
 * readCount = 0; // while (readCount <= 1) { // readCount +=
 * telnet.in.read(bytes); // } // inputstream may be slower than read(byte[])
 * Thread.sleep(500); System.out.println("test " + telnet.in.read(bytes));
 * System.out.println(new String(bytes)); // System.out.println("test " +
 * telnet.in.read(bytes)); // System.out.println(new String(bytes));
 * 
 * // telnet.sendCommand("STA"); // substitute TA telnet.sendCommand("SQUOTE");
 * telnet.sendCommand("UQUOTE"); telnet.sendCommand("QUIT");
 * System.out.println("显示结果"); logger.info("raw data extraction ended"); //
 * telnet.disconnect(); } catch (Exception e) { logger.catching(e);
 * e.printStackTrace(); }
 * 
 * }
 * 
 * }
 **/
