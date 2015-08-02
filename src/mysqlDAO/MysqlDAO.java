package mysqlDAO;

import java.sql.PreparedStatement;

public interface MysqlDAO {
	public boolean insert(String[] record);
	public void insertForTradingSentiment(String[] record, PreparedStatement prepareStatement);
	public void insertForQUOTE(String[] record, PreparedStatement prepareStatement);

}
