import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.concurrent.ThreadLocalRandom;

import static java.lang.System.currentTimeMillis;

public class TestCases {
    // JDBC driver name and database URL
    static final String JDBC_DRIVER = "org.h2.Driver";
    static final String DB_URL = "jdbc:h2:~/test";

    //  Database credentials
    static final String USER = "";
    static final String PASS = "";
    private Connection conn = null;
    private Statement stmt = null;

    private void setConnection(){
        try {
            Class.forName(JDBC_DRIVER);
            conn = DriverManager.getConnection(DB_URL,USER,PASS);
            stmt = conn.createStatement();

        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        } catch (SQLException e) {
            e.printStackTrace();
        }

    }

    private void createTableQuery(){

        try {
            String sql =  "CREATE TABLE   STUDENT " +
                    "(id INTEGER not NULL AUTO_INCREMENT, " +
                    " name VARCHAR(255), " +
                    " age INTEGER, " +
                    " PRIMARY KEY ( id ))";
            stmt.executeUpdate(sql);
            stmt.close();

        } catch(SQLException se) {
            se.printStackTrace();
        } catch(Exception e) {
            e.printStackTrace();
        } finally {
            try{
                if(stmt!=null) stmt.close();
            } catch(SQLException se2) {
            }
            try {
                if(conn!=null) conn.close();
            } catch(SQLException se){
                se.printStackTrace();
            }
        }
    }


    private void insertQuery(int n){

        long init = currentTimeMillis();
        try {
            for (int i = 0; i < n; i++) {
                String sql = "INSERT INTO STUDENT(name,age) " + "VALUES ('Zara', 18)";
                stmt.executeUpdate(sql);
            }
            long end = currentTimeMillis();
            System.out.print("For "+n + " -->  insert: " + Long.toString(end - init) + "   ----    ");
            stmt.close();

        } catch(SQLException se) {
            se.printStackTrace();
        } catch(Exception e) {
            e.printStackTrace();
        } finally {
            try{
                if(stmt!=null) stmt.close();
            } catch(SQLException se2) {
            }
            try {
                if(conn!=null) conn.close();
            } catch(SQLException se){
                se.printStackTrace();
            }
        }
    }

    private void updateQuery(int n){
        int randomNum=0;
        long init = currentTimeMillis();
        try {
            for (int i = 0; i < n; i++) {
                randomNum = ThreadLocalRandom.current().nextInt(0, n);
                String sql =  "UPDATE STUDENT SET name='Hi' WHERE id="+ randomNum ;
                stmt.executeUpdate(sql);
            }
            long end = currentTimeMillis();
            System.out.print("For "+n + " -->  update: " + Long.toString(end - init) + "   ----    ");
            stmt.close();

        } catch(SQLException se) {
            se.printStackTrace();
        } catch(Exception e) {
            e.printStackTrace();
        } finally {
            try{
                if(stmt!=null) stmt.close();
            } catch(SQLException se2) {
            }
            try {
                if(conn!=null) conn.close();
            } catch(SQLException se){
                se.printStackTrace();
            }
        }
    }

    private void deleteQuery(int n){
        int randomNum=0;
        long init = currentTimeMillis();
        try {
            for (int i = 0; i < n; i++) {
                randomNum = ThreadLocalRandom.current().nextInt(0, n);
                String sql =  "DELETE FROM STUDENT WHERE id="+randomNum;
                stmt.executeUpdate(sql);
            }
            long end = currentTimeMillis();
            System.out.print("For "+n + " -->  delete: " + Long.toString(end - init) + "   ----    ");
            stmt.close();
        } catch(SQLException se) {
            se.printStackTrace();
        } catch(Exception e) {
            e.printStackTrace();
        } finally {
            try{
                if(stmt!=null) stmt.close();
            } catch(SQLException se2) {
            }
            try {
                if(conn!=null) conn.close();
            } catch(SQLException se){
                se.printStackTrace();
            }
        }
    }

    private void searchQuery(int n){

        int randomNum=0;
        long init = currentTimeMillis();
        try {
            for (int i = 0; i < n; i++) {
                randomNum = ThreadLocalRandom.current().nextInt(0, n);
                String sql =  "SELECT name FROM STUDENT where id="+randomNum;
                stmt.executeUpdate(sql);
            }
            long end = currentTimeMillis();
            System.out.print("For "+n + " -->  select: " + Long.toString(end - init) + "   ----    ");
            stmt.close();

        } catch(SQLException se) {
            se.printStackTrace();
        } catch(Exception e) {
            e.printStackTrace();
        } finally {
            try{
                if(stmt!=null) stmt.close();
            } catch(SQLException se2) {
            }
            try {
                if(conn!=null) conn.close();
            } catch(SQLException se){
                se.printStackTrace();
            }
        }
    }

    private void createIndex(int n){

        try {
            for (int i = 0; i < n; i++) {
                String sql =  "CREATE INDEX indx1 ON STUDENT(name)";
                stmt.executeUpdate(sql);
            }
            stmt.close();

        } catch(SQLException se) {
            se.printStackTrace();
        } catch(Exception e) {
            e.printStackTrace();
        } finally {
            try{
                if(stmt!=null) stmt.close();
            } catch(SQLException se2) {
            }
            try {
                if(conn!=null) conn.close();
            } catch(SQLException se){
                se.printStackTrace();
            }
        }
    }


    public static void main(String[] args){
        TestCases t = new TestCases();
        t.setConnection();
        t.createTableQuery();

        t.createIndex(5);

        t.insertQuery(5);
        t.updateQuery(5);
        t.deleteQuery(5);
        t.searchQuery(5);

    }

}
