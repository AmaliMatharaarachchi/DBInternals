import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;

public class IndexDetector {

    // JDBC driver name and database URL
    static final String JDBC_DRIVER = "org.h2.Driver";
    static final String DB_URL = "jdbc:h2:~/test";

    //  Database credentials
    static final String USER = "";
    static final String PASS = "";

    void createIndex(String indexName, String tableName, String[] columns){

        Connection conn = null;
        Statement stmt = null;

        if(IndexDetector.detectRedundantIndexes()){
            String a;
        } else {
            try {
                // STEP 1: Register JDBC driver
                Class.forName(JDBC_DRIVER);

                //STEP 2: Open a connection
                System.out.println("Connecting to database...");
                conn = DriverManager.getConnection(DB_URL,USER,PASS);

                //STEP 3: Execute a query
                System.out.println("Creating table in given database...");
                stmt = conn.createStatement();

                String columnNames = "";

                for(String column : columns){
                    columnNames = columnNames.concat(column + ",");
                }

                String sql =  "CREATE INDEX " + indexName + " ON " + tableName +
                        "(" + columnNames + ")";

                stmt.executeUpdate(sql);
                System.out.println("Created table in given database...");

                // STEP 4: Clean-up environment
                stmt.close();
                conn.close();
            } catch(SQLException se) {
                //Handle errors for JDBC
                se.printStackTrace();
            } catch(Exception e) {
                //Handle errors for Class.forName
                e.printStackTrace();
            } finally {
                //finally block used to close resources
                try{
                    if(stmt!=null) stmt.close();
                } catch(SQLException se2) {
                } // nothing we can do
                try {
                    if(conn!=null) conn.close();
                } catch(SQLException se){
                    se.printStackTrace();
                } //end finally try
            } //end try
            System.out.println("Goodbye!");
        }


    }

    private static boolean detectRedundantIndexes(){ //check return type
        return true;
    }

    public static void main(String[] args){

    }
}
