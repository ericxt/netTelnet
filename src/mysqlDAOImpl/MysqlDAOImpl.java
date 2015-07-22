package mysqlDAOImpl;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Timestamp;

import mysqlDAO.MysqlDAO;

import com.mysql.jdbc.Connection;
import com.sun.prism.Presentable;

import dBDriverUtil.MysqlDBUtil;

public class MysqlDAOImpl implements MysqlDAO {

	public boolean insert(String[] record) {
		// TODO Auto-generated method stub
		Connection conn = MysqlDBUtil.getConnection();
		String sql = "insert into index_ta(Flag,TransactionTime,ContractId,TAIndex,Buy1Price,"
				+ "Buy2Price,Buy3Price,Buy4Price,Buy5Price,Buy1Num,Buy2Num,Buy3Num,Buy4Num,"
				+ "Buy5Num,Sell1Price,Sell2Price,Sell3Price,Sell4Price,Sell5Price,Sell1Num,"
				+ "Sell2Num,Sell3Num,Sell4Num,Sell5Num,m_dZJSJ,m_dJJSJ,m_dCJJJ,m_dZSP,m_dJSP,"
				+ "m_dJKP,m_nZCCL,m_nCCL,m_dZXJ,m_nCJSL,m_dCJJE,m_dZGBJ,m_dZDBJ,m_dZGJ,m_dZDJ,m_dZXSD,m_dJXSD) "
				+ "value(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)";

		try {
			PreparedStatement prepareStatement = conn.prepareStatement(sql);

			for (int i = 0; i < 3; i++) {
				prepareStatement.setString(i + 1, record[i]);
			}
			for (int i = 0; i < 6; i++) {
				prepareStatement.setFloat(i + 4,
						Float.parseFloat(record[i + 3]));
			}
			for (int i = 0; i < 5; i++) {
				prepareStatement
						.setLong(i + 10, Long.parseLong(record[i + 9]));
			}
			for (int i = 0; i < 5; i++) {
				prepareStatement.setFloat(i + 15,
						Float.parseFloat(record[i + 14]));
			}
			for (int i = 0; i < 5; i++) {
				prepareStatement.setLong(i + 20,
						Long.parseLong(record[i + 19]));
			}
			for (int i = 0; i < 6; i++) {
				prepareStatement.setFloat(i + 25,
						Float.parseFloat(record[i + 24]));
			}
			for (int i = 0; i < 2; i++) {
				prepareStatement.setLong(i + 31,
						Long.parseLong(record[i + 30]));
			}
			prepareStatement.setFloat(33, Float.parseFloat(record[32]));
			prepareStatement.setLong(34, Long.parseLong(record[33]));
			for (int i = 0; i < 7; i++) {
				prepareStatement.setFloat(i + 35,
						Float.parseFloat(record[i + 34]));
			}
			prepareStatement.executeUpdate();

		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return false;
	}

	public void insertForTradingSentiment(String[] record,
			PreparedStatement prepareStatement) {
		// TODO Auto-generated method stub
		try {
			prepareStatement.setTimestamp(1, Timestamp.valueOf(record[0]));
			prepareStatement.setString(2, record[1]);
			prepareStatement.setFloat(3, Float.parseFloat(record[2]));
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	public void insertForQUOTE(String[] record,
			PreparedStatement preparedStatement) {
		System.out.println("turnover >>> " + record[13] + ", " + new BigDecimal(record[13]));
		try {
			preparedStatement.setString(1, record[0]);
			preparedStatement.setString(2, record[1]);
			preparedStatement.setString(3, record[2]);
			for (int i = 3; i < 9; i++) {
				preparedStatement.setBigDecimal(i + 1, new BigDecimal(record[i]));
			}
			preparedStatement.setLong(10, Long.parseLong(record[9]));
			preparedStatement.setLong(11, Long.parseLong(record[10]));
			preparedStatement.setBigDecimal(12, new BigDecimal(record[11]));
			preparedStatement.setLong(13, Long.parseLong(record[12]));
			for (int i = 13; i < 22; i++) {
				preparedStatement.setBigDecimal(i + 1, new BigDecimal(record[i]));
			}
			preparedStatement.setLong(23, Long.parseLong(record[22]));
			preparedStatement.setLong(24, Long.parseLong(record[23]));
			preparedStatement.setBigDecimal(25, new BigDecimal(record[24]));
			preparedStatement.setBigDecimal(26, new BigDecimal(record[25]));
			preparedStatement.setLong(27, Long.parseLong(record[26]));
			preparedStatement.setLong(28, Long.parseLong(record[27]));
			preparedStatement.setBigDecimal(29, new BigDecimal(record[28]));
			preparedStatement.setBigDecimal(30, new BigDecimal(record[29]));
			preparedStatement.setLong(31, Long.parseLong(record[30]));
			preparedStatement.setLong(32, Long.parseLong(record[31]));
			preparedStatement.setBigDecimal(33, new BigDecimal(record[32]));
			preparedStatement.setBigDecimal(34, new BigDecimal(record[33]));
			preparedStatement.setLong(35, Long.parseLong(record[34]));
			preparedStatement.setLong(36, Long.parseLong(record[35]));
			preparedStatement.setBigDecimal(37, new BigDecimal(record[36]));
			preparedStatement.setBigDecimal(38, new BigDecimal(record[37]));
			preparedStatement.setLong(39, Long.parseLong(record[38]));
			preparedStatement.setLong(40, Long.parseLong(record[39]));

		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
}
