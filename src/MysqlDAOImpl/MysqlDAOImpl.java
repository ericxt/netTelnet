package MysqlDAOImpl;

import java.sql.PreparedStatement;
import java.sql.SQLException;

import com.mysql.jdbc.Connection;

import DBDriverUtil.MysqlDBUtil;
import MysqlDAO.MysqlDAO;

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
						.setInt(i + 10, Integer.parseInt(record[i + 9]));
			}
			for (int i = 0; i < 5; i++) {
				prepareStatement.setFloat(i + 15,
						Float.parseFloat(record[i + 14]));
			}
			for (int i = 0; i < 5; i++) {
				prepareStatement.setInt(i + 20,
						Integer.parseInt(record[i + 19]));
			}
			for (int i = 0; i < 6; i++) {
				prepareStatement.setFloat(i + 25,
						Float.parseFloat(record[i + 24]));
			}
			for (int i = 0; i < 2; i++) {
				prepareStatement.setInt(i + 31,
						Integer.parseInt(record[i + 30]));
			}
			prepareStatement.setFloat(33, Float.parseFloat(record[32]));
			prepareStatement.setInt(34, Integer.parseInt(record[33]));
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

	public void insertForTA(String[] record, PreparedStatement prepareStatement) {
		// TODO Auto-generated method stub

		for (int i = 0; i < 3; i++) {
			try {
				prepareStatement.setString(i + 1, record[i]);
			} catch (SQLException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		for (int i = 0; i < 6; i++) {
			try {
				prepareStatement.setFloat(i + 4,
						Float.parseFloat(record[i + 3]));
			} catch (NumberFormatException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (SQLException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		for (int i = 0; i < 5; i++) {
			try {
				prepareStatement
						.setInt(i + 10, Integer.parseInt(record[i + 9]));
			} catch (NumberFormatException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (SQLException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		for (int i = 0; i < 5; i++) {
			try {
				prepareStatement.setFloat(i + 15,
						Float.parseFloat(record[i + 14]));
			} catch (NumberFormatException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (SQLException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		for (int i = 0; i < 5; i++) {
			try {
				prepareStatement.setInt(i + 20,
						Integer.parseInt(record[i + 19]));
			} catch (NumberFormatException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (SQLException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		for (int i = 0; i < 6; i++) {
			try {
				prepareStatement.setFloat(i + 25,
						Float.parseFloat(record[i + 24]));
			} catch (NumberFormatException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (SQLException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		for (int i = 0; i < 2; i++) {
			try {
				prepareStatement.setInt(i + 31,
						Integer.parseInt(record[i + 30]));
			} catch (NumberFormatException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (SQLException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		try {
			prepareStatement.setFloat(33, Float.parseFloat(record[32]));
		} catch (NumberFormatException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		try {
			prepareStatement.setInt(34, Integer.parseInt(record[33]));
		} catch (NumberFormatException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		for (int i = 0; i < 7; i++) {
			try {
				prepareStatement.setFloat(i + 35,
						Float.parseFloat(record[i + 34]));
			} catch (NumberFormatException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (SQLException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	}

	public void insertForQUOTE(String[] record,
			PreparedStatement preparedStatement) {
		try {
			preparedStatement.setString(1, record[0]);
			preparedStatement.setString(2, record[1]);
			preparedStatement.setString(3, record[2]);
			for (int i = 3; i < 9; i++) {
				preparedStatement.setFloat(i + 1, Float.parseFloat(record[i]));
			}
			preparedStatement.setInt(10, Integer.parseInt(record[9]));
			preparedStatement.setInt(11, Integer.parseInt(record[10]));
			preparedStatement.setFloat(12, Float.parseFloat(record[11]));
			preparedStatement.setInt(13, Integer.parseInt(record[12]));
			for (int i = 13; i < 22; i++) {
				preparedStatement.setFloat(i + 1, Float.parseFloat(record[i]));
			}
			preparedStatement.setInt(23, Integer.parseInt(record[22]));
			preparedStatement.setInt(24, Integer.parseInt(record[23]));
			preparedStatement.setFloat(25, Float.parseFloat(record[24]));
			preparedStatement.setFloat(26, Float.parseFloat(record[25]));
			preparedStatement.setInt(27, Integer.parseInt(record[26]));
			preparedStatement.setInt(28, Integer.parseInt(record[27]));
			preparedStatement.setFloat(29, Float.parseFloat(record[28]));
			preparedStatement.setFloat(30, Float.parseFloat(record[29]));
			preparedStatement.setInt(31, Integer.parseInt(record[30]));
			preparedStatement.setInt(32, Integer.parseInt(record[31]));
			preparedStatement.setFloat(33, Float.parseFloat(record[32]));
			preparedStatement.setFloat(34, Float.parseFloat(record[33]));
			preparedStatement.setInt(35, Integer.parseInt(record[34]));
			preparedStatement.setInt(36, Integer.parseInt(record[35]));
			preparedStatement.setFloat(37, Float.parseFloat(record[36]));
			preparedStatement.setFloat(38, Float.parseFloat(record[37]));
			preparedStatement.setInt(39, Integer.parseInt(record[38]));
			preparedStatement.setInt(40, Integer.parseInt(record[39]));

		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
}
