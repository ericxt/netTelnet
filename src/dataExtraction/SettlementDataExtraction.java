package dataExtraction;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import com.mysql.jdbc.Connection;

public class SettlementDataExtraction {
	private Connection conn = null;
	private ResultSet resultSet = null;

	private boolean futuresStatementCreated = false;
	private boolean debtStatementCreated = false;
	private boolean indexStatementCreated = false;
	private boolean stockStatementCreated = false;

	public SettlementDataExtraction() {
		// TODO Auto-generated constructor stub
	}

	public SettlementDataExtraction(Connection conn) {
		this.conn = conn;
	}

	public SettlementDataExtraction(ResultSet resultSet) {
		this.resultSet = resultSet;
	}

	public SettlementDataExtraction(Connection conn, ResultSet resultSet) {
		this.conn = conn;
		this.resultSet = resultSet;
	}

	public void operate() {
		// String debtSourceSql =
		// "select * from (select ContractId as Ticker, date(TradingTime) as TradingDate, "
		// +
		// "PreSettlementPrice, PreClosePrice, CurrOpenPrice as curOpenPrice, PreHoldings, Volume "
		// + "from xcube.debt_quotation as a "
		// +
		// "where TradingTime=(select TradingTime from xcube.latest_debt_tradingtime "
		// + "where a.ContractId=contractid)) as b group by Ticker";

		final String debtSourceSql = "select Ticker, TradingDate, PreSettlementPrice, PreClosePrice, "
				+ "curOpenPrice, PreHoldings, Volume "
				+ "from (select ContractId as Ticker, date(TradingTime) as TradingDate, "
				+ "PreSettlementPrice, PreClosePrice, CurrOpenPrice as curOpenPrice, PreHoldings, "
				+ "Volume "
				+ "from xcube.debt_quotation order by tradingtime desc) as a "
				+ "group by Ticker;";

		// String futuresSourceSql =
		// "select * from (select ContractId as Ticker, date(TradingTime) as TradingDate, "
		// +
		// "PreSettlementPrice, PreClosePrice, CurrOpenPrice as curOpenPrice, PreHoldings, Volume "
		// + "from xcube.futures_quotation as a "
		// +
		// "where TradingTime=(select TradingTime from xcube.latest_futures_tradingtime "
		// + "where a.ContractId=contractid)) as b group by Ticker";

		final String futuresSourceSql = "select Ticker, TradingDate, PreSettlementPrice, PreClosePrice, "
				+ "curOpenPrice, PreHoldings, Volume "
				+ "from (select ContractId as Ticker, date(TradingTime) as TradingDate, "
				+ "PreSettlementPrice, PreClosePrice, CurrOpenPrice as curOpenPrice, PreHoldings, "
				+ "Volume "
				+ "from xcube.futures_quotation order by tradingtime desc) as a "
				+ "group by Ticker;";

		// String indexSourceSql =
		// "select * from (select ContractId as Ticker, date(TradingTime) as TradingDate, "
		// +
		// "PreSettlementPrice, PreClosePrice, CurrOpenPrice as curOpenPrice, PreHoldings, Volume "
		// + "from xcube.index_quotation as a "
		// +
		// "where TradingTime=(select TradingTime from xcube.latest_index_tradingtime "
		// + "where a.ContractId=contractid)) as b group by Ticker";

		final String indexSourceSql = "select Ticker, TradingDate, PreSettlementPrice, PreClosePrice, "
				+ "curOpenPrice, PreHoldings, Volume "
				+ "from (select ContractId as Ticker, date(TradingTime) as TradingDate, "
				+ "PreSettlementPrice, PreClosePrice, CurrOpenPrice as curOpenPrice, PreHoldings, "
				+ "Volume "
				+ "from xcube.index_quotation order by tradingtime desc) as a "
				+ "group by Ticker;";

		// String stockSourceSql =
		// "select * from (select ContractId as Ticker, date(TradingTime) as TradingDate, "
		// +
		// "PreSettlementPrice, PreClosePrice, CurrOpenPrice as curOpenPrice, PreHoldings, Volume "
		// + "from xcube.stock_quotation as a "
		// +
		// "where TradingTime=(select TradingTime from xcube.latest_stock_tradingtime "
		// + "where a.ContractId=contractid)) as b group by Ticker";

		final String stockSourceSql = "select Ticker, TradingDate, PreSettlementPrice, PreClosePrice, "
				+ "curOpenPrice, PreHoldings, Volume "
				+ "from (select ContractId as Ticker, date(TradingTime) as TradingDate, "
				+ "PreSettlementPrice, PreClosePrice, CurrOpenPrice as curOpenPrice, PreHoldings, "
				+ "Volume "
				+ "from xcube.stock_quotation order by tradingtime desc limit 20000) as a "
				+ "group by Ticker;";

		String targetSql = "replace into xcube.settlement_data(ticker, tradingDate, preSettlementPrice, "
				+ "preClosePrice, curOpenPrice, preHoldings, volume) values(?, ?, ?, ?, ?, ?, ?)";

		// parameters in settlement_data talbe
		String ticker = null;
		Date tradingDate = null;
		BigDecimal preSettlementPrice = BigDecimal.ZERO;
		BigDecimal preClosePrice = BigDecimal.ZERO;
		BigDecimal curOpenPrice = BigDecimal.ZERO;
		BigInteger preHoldings = BigInteger.ZERO;
		BigInteger volume = BigInteger.ZERO;

		if (conn == null) {
			System.out
					.println("SettlementDataExtraction.operate >>> reconstruct conn");
			conn = dBDriverUtil.MysqlDBUtil.getConnection();
		}

		try {
			final PreparedStatement debtTargetPrestmt = conn
					.prepareStatement(targetSql);
			final PreparedStatement futuresTargetPrestmt = conn
					.prepareStatement(targetSql);
			final PreparedStatement indexTargetPrestmt = conn
					.prepareStatement(targetSql);
			final PreparedStatement stockTargetPrestmt = conn
					.prepareStatement(targetSql);

			System.out.println("Threads invoking ...");

			// debtThread
			Thread debtThread = new Thread(new Runnable() {

				public void run() {
					try {
						PreparedStatement debtPrestmt = conn.prepareStatement(debtSourceSql);
						final ResultSet debtRs = debtPrestmt.executeQuery();
						System.out.println("debt thread");
						while (debtRs.next()) {
							String ticker = debtRs.getString("Ticker");
							Date tradingDate = debtRs.getDate("TradingDate");
							BigDecimal preSettlementPrice = debtRs
									.getBigDecimal("preSettlementPrice");
							BigDecimal preClosePrice = debtRs
									.getBigDecimal("preClosePrice");
							BigDecimal curOpenPrice = debtRs
									.getBigDecimal("curOpenPrice");
							BigInteger preHoldings = BigInteger.valueOf(debtRs
									.getLong("preHoldings"));
							BigInteger volume = BigInteger.valueOf(debtRs
									.getLong("volume"));

							System.out.println("DebtRecord " + debtRs.getRow()
									+ " >>> " + ticker + ", " + tradingDate
									+ ", " + preSettlementPrice + ", "
									+ preClosePrice + ", " + curOpenPrice
									+ ", " + preHoldings + ", " + volume);

							debtTargetPrestmt.setString(1, ticker);
							debtTargetPrestmt.setDate(2, tradingDate);
							debtTargetPrestmt.setBigDecimal(3,
									preSettlementPrice);
							debtTargetPrestmt.setBigDecimal(4, preClosePrice);
							debtTargetPrestmt.setBigDecimal(5, curOpenPrice);
							debtTargetPrestmt.setLong(6,
									preHoldings.longValue());
							debtTargetPrestmt.setLong(7, volume.longValue());

							debtTargetPrestmt.execute();
						}

						debtTargetPrestmt.close();
						debtStatementCreated = true;
					} catch (SQLException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
				}
			}, "DebtDataHandle");
			System.out.println("invoke debtThread");
			debtThread.start();

			// futuresThread
			Thread futuresThread = new Thread(new Runnable() {

				public void run() {
					// TODO Auto-generated method stub
					try {
						PreparedStatement futuresPrestmt = conn.prepareStatement(futuresSourceSql);
						final ResultSet futuresRs = futuresPrestmt
								.executeQuery();
						while (futuresRs.next()) {
							String ticker = futuresRs.getString("Ticker");
							Date tradingDate = futuresRs.getDate("TradingDate");
							BigDecimal preSettlementPrice = futuresRs
									.getBigDecimal("preSettlementPrice");
							BigDecimal preClosePrice = futuresRs
									.getBigDecimal("preClosePrice");
							BigDecimal curOpenPrice = futuresRs
									.getBigDecimal("curOpenPrice");
							BigInteger preHoldings = BigInteger
									.valueOf(futuresRs.getLong("preHoldings"));
							BigInteger volume = BigInteger.valueOf(futuresRs
									.getLong("volume"));

							System.out.println("FuturesRecord "
									+ futuresRs.getRow() + " >>> " + ticker
									+ ", " + tradingDate + ", "
									+ preSettlementPrice + ", " + preClosePrice
									+ ", " + curOpenPrice + ", " + preHoldings
									+ ", " + volume);

							futuresTargetPrestmt.setString(1, ticker);
							futuresTargetPrestmt.setDate(2, tradingDate);
							futuresTargetPrestmt.setBigDecimal(3,
									preSettlementPrice);
							futuresTargetPrestmt
									.setBigDecimal(4, preClosePrice);
							futuresTargetPrestmt.setBigDecimal(5, curOpenPrice);
							futuresTargetPrestmt.setLong(6,
									preHoldings.longValue());
							futuresTargetPrestmt.setLong(7, volume.longValue());

							futuresTargetPrestmt.execute();
						}

						futuresTargetPrestmt.close();

						futuresStatementCreated = true;
					} catch (SQLException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
				}
			}, "FuturesDataHandle");
			futuresThread.start();

			// indexThread
			Thread indexThread = new Thread(new Runnable() {

				public void run() {
					// TODO Auto-generated method stub
					try {
						PreparedStatement indexPrestmt = conn.prepareStatement(indexSourceSql);
						final ResultSet indexRs = indexPrestmt.executeQuery();
						while (indexRs.next()) {
							String ticker = indexRs.getString("Ticker");
							Date tradingDate = indexRs.getDate("TradingDate");
							BigDecimal preSettlementPrice = indexRs
									.getBigDecimal("preSettlementPrice");
							BigDecimal preClosePrice = indexRs
									.getBigDecimal("preClosePrice");
							BigDecimal curOpenPrice = indexRs
									.getBigDecimal("curOpenPrice");
							BigInteger preHoldings = BigInteger.valueOf(indexRs
									.getLong("preHoldings"));
							BigInteger volume = BigInteger.valueOf(indexRs
									.getLong("volume"));

							System.out.println("IndexRecord "
									+ indexRs.getRow() + " >>> " + ticker
									+ ", " + tradingDate + ", "
									+ preSettlementPrice + ", " + preClosePrice
									+ ", " + curOpenPrice + ", " + preHoldings
									+ ", " + volume);

							indexTargetPrestmt.setString(1, ticker);
							indexTargetPrestmt.setDate(2, tradingDate);
							indexTargetPrestmt.setBigDecimal(3,
									preSettlementPrice);
							indexTargetPrestmt.setBigDecimal(4, preClosePrice);
							indexTargetPrestmt.setBigDecimal(5, curOpenPrice);
							indexTargetPrestmt.setLong(6,
									preHoldings.longValue());
							indexTargetPrestmt.setLong(7, volume.longValue());

							indexTargetPrestmt.execute();
						}

						indexTargetPrestmt.close();

						indexStatementCreated = true;
					} catch (SQLException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
				}
			}, "IndexDataHandle");
			indexThread.start();

			// stockThread
			Thread stockThread = new Thread(new Runnable() {

				public void run() {
					// TODO Auto-generated method stub
					try {
						PreparedStatement stockPrestmt = conn.prepareStatement(stockSourceSql);
						final ResultSet stockRs = stockPrestmt.executeQuery();
						while (stockRs.next()) {
							String ticker = stockRs.getString("Ticker");
							Date tradingDate = stockRs.getDate("TradingDate");
							BigDecimal preSettlementPrice = stockRs
									.getBigDecimal("preSettlementPrice");
							BigDecimal preClosePrice = stockRs
									.getBigDecimal("preClosePrice");
							BigDecimal curOpenPrice = stockRs
									.getBigDecimal("curOpenPrice");
							BigInteger preHoldings = BigInteger.valueOf(stockRs
									.getLong("preHoldings"));
							BigInteger volume = BigInteger.valueOf(stockRs
									.getLong("volume"));

							System.out.println("StockRecord "
									+ stockRs.getRow() + " >>> " + ticker
									+ ", " + tradingDate + ", "
									+ preSettlementPrice + ", " + preClosePrice
									+ ", " + curOpenPrice + ", " + preHoldings
									+ ", " + volume);

							stockTargetPrestmt.setString(1, ticker);
							stockTargetPrestmt.setDate(2, tradingDate);
							stockTargetPrestmt.setBigDecimal(3,
									preSettlementPrice);
							stockTargetPrestmt.setBigDecimal(4, preClosePrice);
							stockTargetPrestmt.setBigDecimal(5, curOpenPrice);
							stockTargetPrestmt.setLong(6,
									preHoldings.longValue());
							stockTargetPrestmt.setLong(7, volume.longValue());

							stockTargetPrestmt.execute();
						}

						stockTargetPrestmt.close();
						stockStatementCreated = true;
					} catch (SQLException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
				}
			}, "StockDataHandle");
			stockThread.start();

			System.out.println("connCloseOperation starting");
			Thread connCloseOperation = new Thread(new Runnable() {

				public void run() {
					// TODO Auto-generated method stub
					while (true) {
						if (allStatementsClosed()) {
							try {
								System.out.println("close connection");
								conn.close();
								break;
							} catch (SQLException e) {
								// TODO Auto-generated catch block
								e.printStackTrace();
							}
						}
					}
				}
			}, "connCloseOperationThread");
			connCloseOperation.start();
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}

	private boolean allStatementsClosed() {
		// TODO Auto-generated method stub
		if (futuresStatementCreated && debtStatementCreated
				&& indexStatementCreated && stockStatementCreated) {
			return true;
		}
		return false;
	}

}
