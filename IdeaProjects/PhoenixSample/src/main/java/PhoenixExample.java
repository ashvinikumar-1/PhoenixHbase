import java.sql.*;

/**
 * Created by ashvinikumar on 1/8/17.
 */
public class PhoenixExample {
    public static void main(String[] args) throws SQLException {
        // Create variables
        Connection connection = null;
        Statement statement = null;
        ResultSet rs = null;
        PreparedStatement ps = null;


            // Connect to the database
            connection = DriverManager.getConnection("jdbc:phoenix:localhost");

            // Create a JDBC statement
            statement = connection.createStatement();

//            // Execute our statements
//            statement.executeUpdate("create table javatest (mykey integer not null primary key, mycolumn varchar)");
//            statement.executeUpdate("upsert into javatest values (1,'Hello')");
//            statement.executeUpdate("upsert into javatest values (2,'Java Application')");
//            connection.commit();

            // Query for table
            ps = connection.prepareStatement("select * from javatest");
            rs = ps.executeQuery();
            System.out.println("Table Values");
            while(rs.next()) {
                Integer myKey = rs.getInt(1);
                String myColumn = rs.getString(2);
                System.out.println("\tRow: " + myKey + " = " + myColumn);
            }



    }
}
