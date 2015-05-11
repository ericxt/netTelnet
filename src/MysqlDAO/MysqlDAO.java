package MysqlDAO;

import java.sql.PreparedStatement;

import com.mysql.jdbc.Connection;

public interface MysqlDAO {
	public boolean insert(String[] record);
	public void insertForTA(String[] record, PreparedStatement prepareStatement);
	public void insertForQUOTE(String[] record, PreparedStatement prepareStatement);

}
