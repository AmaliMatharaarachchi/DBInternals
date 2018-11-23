import java.sql.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class IndexDetector {

    // JDBC driver name and database URL
    static final String JDBC_DRIVER = "org.h2.Driver";
    static final String DB_URL = "jdbc:h2:~/test";

    //  Database credentials
    static final String USER = "";
    static final String PASS = "";
    private static Connection conn = null;
    private static Statement stmt = null;

    private static void setConnection(){
        try {
            // STEP 1: Register JDBC driver
            Class.forName(JDBC_DRIVER);

            //STEP 2: Open a connection
            System.out.println("Connecting to database...");
            conn = DriverManager.getConnection(DB_URL,USER,PASS);

            //STEP 3: Execute a query
            System.out.println("Creating table in given database...");
            stmt = conn.createStatement();

        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        } catch (SQLException e) {
            e.printStackTrace();
        }

    }

    private static void createIndex(String indexName, String tableName, List<String> columns){

        try {

            String columnNames = "";

            for(int i=0; i < columns.size()-1;i++){
                columnNames = columnNames.concat(columns.get(i) + ",");
            }

            columnNames = columnNames.concat(columns.get(columns.size()-1));

            String sql =  "CREATE INDEX " + indexName + " ON " + tableName +
                    "(" + columnNames + ")";

            stmt.executeUpdate(sql);
            System.out.println("Created table in given database...");

            // STEP 4: Clean-up environment
            stmt.close();

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

    public static Map<String, String> getIndexes() {
        Map<String, String> indexes = new HashMap<String, String>();
//        Statement stmt = null;
//        stmt = conn.createStatement();
        String sql = " SELECT s.INDEXED_COL,GROUP_CONCAT(INDEX_NAME) INDEX_NAMES FROM (" +
                "SELECT INDEX_NAME,GROUP_CONCAT(CONCAT(TABLE_NAME,'.',COLUMN_NAME) ORDER BY CONCAT(ORDINAL_POSITION,COLUMN_NAME)) INDEXED_COL FROM INFORMATION_SCHEMA.indexes WHERE table_schema = 'PUBLIC'" +
                "AND table_name='TEST_H2' GROUP BY INDEX_NAME" +
                ")as s GROUP BY INDEXED_COL HAVING COUNT(1)>1";


        ResultSet rs = null;

        try {
            Map<String, String> temp = new HashMap<String, String>();
            rs = stmt.executeQuery(sql);
            while (rs.next()) {
                String i = rs.getString("INDEXED_COL");
                String j = rs.getString("INDEX_NAMES");

                temp.put(i.toLowerCase(), j.toLowerCase());
            }
            for (Map.Entry<String, String> i : temp.entrySet()) {
                String columns = i.getKey();
                String index_name = i.getValue();

                String[] index_columns = columns.split(",");
//                String[] index_names= names.split(",");

                for (String index_column : index_columns) {
                    indexes.put(index_column, index_name);
                }

            }


        } catch (SQLException e) {
            e.printStackTrace();
        }

        return indexes;

    }

    public static void detectRedundantIndexes(String indexName, String tableName, List<String> columns) {
        Map<String, String> indexes = getIndexes();
        System.out.println(indexes.toString());

        List<String> index_columns= new ArrayList<String>();
        for (String column : columns) {
            System.out.println(column);
            System.out.println(tableName+"."+column);
            if (indexes.containsKey(tableName.toLowerCase()+"."+column.toLowerCase())) {
                System.out.println("index for "+column+" already exists as :" + indexes.get(tableName.toLowerCase()+"."+column.toLowerCase()));

            }
            else{
                index_columns.add(column);
            }

        }
        if (index_columns.size()>0){
            createIndex( indexName,  tableName, index_columns);
        }

    }

    public static void main(String[] args){
//        IndexDetector i = new IndexDetector();
        setConnection();
        ArrayList<String> a = new ArrayList<>();
        a.add("first");
        a.add("last");
//        String a[] = {"first", "last"};
        IndexDetector.detectRedundantIndexes("indexTest9", "TEST_H2", a);
//        IndexDetector.createIndex("indexTest2", "TEST_H2", a);
        try {
            IndexDetector.conn.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }
}
