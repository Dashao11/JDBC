import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
 
/**
 *
 * @author mysqltutorial.org
 */
public class MySQLJDBCUtil {
 
    /**
     * Get database connection
     *
     * @return a Connection object
     * @throws SQLException
     */
    public static Connection getConnection() throws SQLException {
    	String url       = "jdbc:mysql://localhost:3306/mysqljdbc";
    	String user      = "root";
    	String password  = "secret";
    	 
    	Connection conn = null;
    	 
    	
    	String url       = "jdbc:mysql://localhost:3306/mysqljdbc";
    	String user      = "root";
    	String password  = "secret";
    	 
    	Connection conn = null;
    	 
    	try(conn = DriverManager.getConnection(url, user, password);) {
    	 // processing here
    	} catch(SQLException e) {
    	   System.out.println(e.getMessage());
    	}
    }
 
}