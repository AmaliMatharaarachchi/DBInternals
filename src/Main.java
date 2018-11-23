import java.sql.*;

/**
 * Created by ASUS on 2018-11-23.
 */
public class Main {


    static final String JDBC_DRIVER = "org.h2.Driver";
    static final String DB_URL = "jdbc:h2:~/test";

    //  Database credentials
    static final String USER = "";
    static final String PASS = "";

    public static void main(String[] args) {
        Connection conn = null;
        Statement stmt = null;
        try {
            // STEP 1: Register JDBC driver
            Class.forName(JDBC_DRIVER);

            //STEP 2: Open a connection
            System.out.println("Connecting to database...");
            conn = DriverManager.getConnection(DB_URL, USER, PASS);

            //STEP 3: Execute a query
//            System.out.println("Creating table in given database...");
//            stmt = conn.createStatement();
//            String sql = "CREATE TABLE   TEST_H2 " +
//                    "(id INTEGER not NULL, " +
//                    " first VARCHAR(255), " +
//                    " last VARCHAR(255), " +
//                    " age INTEGER, " +
//                    " PRIMARY KEY ( id ))";
//            stmt.executeUpdate(sql);
//            System.out.println("Created table in given database...");

            /////////////////////////////////////

//            System.out.println("Creating table in given database...");
//            stmt = conn.createStatement();
//            String sql = " CREATE INDEX test2 ON TEST_H2(first,last)";
//            stmt.executeUpdate(sql);
//            System.out.println("Created table in given database...");

            System.out.println("Creating table in given database...");
            stmt = conn.createStatement();
            String sql = " SELECT s.INDEXED_COL,GROUP_CONCAT(INDEX_NAME) INDEX_NAMES FROM (" +
                    "SELECT INDEX_NAME,GROUP_CONCAT(CONCAT(TABLE_NAME,'.',COLUMN_NAME) ORDER BY CONCAT(ORDINAL_POSITION,COLUMN_NAME)) INDEXED_COL FROM INFORMATION_SCHEMA.indexes WHERE table_schema = 'PUBLIC'" +
                    "AND table_name='TEST_H2' GROUP BY INDEX_NAME" +
                    ")as s GROUP BY INDEXED_COL HAVING COUNT(1)>1";
//            stmt.executeQuery(sql);

            ResultSet rs = stmt.executeQuery(sql);

            while (rs.next()) {
                String i = rs.getString("INDEXED_COL");
                String j = rs.getString("INDEX_NAMES");

                System.out.println(i + "\t"+j
                       );
            }

            System.out.println("Created table in given database...");

















            ///////////////////////////////////////

            // STEP 4: Clean-up environment
            stmt.close();
            conn.close();
        } catch (SQLException se) {
            //Handle errors for JDBC
            se.printStackTrace();
        } catch (Exception e) {
            //Handle errors for Class.forName
            e.printStackTrace();
        } finally {
            //finally block used to close resources
            try {
                if (stmt != null) stmt.close();
            } catch (SQLException se2) {
            } // nothing we can do
            try {
                if (conn != null) conn.close();
            } catch (SQLException se) {
                se.printStackTrace();
            } //end finally try
        } //end try
        System.out.println("Goodbye!");
    }
}

