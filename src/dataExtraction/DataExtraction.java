package dataExtraction;

import java.util.Calendar;
import java.util.Date;
import java.util.Timer;
import java.util.TimerTask;

import netTelnet.NetTelnet;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class DataExtraction {
	static Logger logger = LogManager.getLogger();

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		Calendar calendar = Calendar.getInstance();
		int year = calendar.get(Calendar.YEAR);
		int month = calendar.get(Calendar.MONTH);
		int day = calendar.get(Calendar.DAY_OF_MONTH);
		calendar.set(year, month, day + 1, 15, 20, 0);
		final Date date = calendar.getTime();
		System.out.println(date);

		final int period = 24 * 60 * 60 * 1000;
		final Timer timer = new Timer();
		final TimerTask tickerDataExtractionTask = new TickerDataExtractionTask();
		final SentimentExtractionTask sentimentExtractionTask = new SentimentExtractionTask();
		
		Thread tickerDataTask = new Thread(new Runnable() {

			public void run() {
				// TODO Auto-generated method stub
				logger.info("run tickerDataTask >>> " + new Date(System.currentTimeMillis()));
				timer.schedule(tickerDataExtractionTask, date, period);

			}
		}, "TickerDataTask");
//		tickerDataTask.start();
		
//		Thread sentimentTask = new Thread(new Runnable() {
//			
//			public void run() {
//				// TODO Auto-generated method stub
//				timer.schedule(sentimentExtractionTask, date, period);
//				
//			}
//		});
//		sentimentTask.start();
		
		new SettlementDataExtraction().operate();
		
	}

}

class TickerDataExtractionTask extends TimerTask {
	static Logger logger = LogManager.getLogger();

	public void run() {
		// TODO Auto-generated method stub
		try {
			System.out.println("start telnet to extract ticker detail data");
			logger.info("start telnet to extract ticker detail data >>> "
					+ new Date(System.currentTimeMillis()));
			String ip = "203.187.171.249";
			int port = 33331;
			String user = "";
			String password = "";
			NetTelnet telnet = new NetTelnet(ip, port, user, password);
			byte[] bytes = new byte[1024];
			
			Thread.sleep(500);
			System.out.println("test " + telnet.in.read(bytes));
			System.out.println(new String(bytes));

			// telnet.sendCommand("STA"); // substitute TA
			telnet.sendCommand("SQUOTE");
			telnet.sendCommand("UQUOTE");
			telnet.sendCommand("QUIT");
			System.out.println("close the connection");
			logger.info("ticker detail data extraction ended");
			// telnet.disconnect();
		} catch (Exception e) {
			logger.catching(e);
			e.printStackTrace();
		}

	}
}

class SentimentExtractionTask extends TimerTask {
	static Logger logger = LogManager.getLogger();

	public void run() {
		// TODO Auto-generated method stub
		try {
			System.out
					.println("start telnet to extract trading sentiment data");
			logger.info("start telnet to extract trading sentiment data >>> "
					+ new Date(System.currentTimeMillis()));
			String ip = "203.187.171.249";
			int port = 33331;
			String user = "";
			String password = "";
			NetTelnet telnet = new NetTelnet(ip, port, user, password);
			byte[] bytes = new byte[1024];
			Thread.sleep(500);
			telnet.in.read(bytes);
			System.out.println("subscriptions >>> " + new String(bytes));

			telnet.sendCommand("STA"); // substitute TA
			telnet.sendCommand("UTA");
			telnet.sendCommand("QUIT");
			System.out.println("close telnet connection");
			logger.info("trading sentiment data extraction ended");
		} catch (Exception e) {
			logger.catching(e);
			e.printStackTrace();
		}

	}
}
