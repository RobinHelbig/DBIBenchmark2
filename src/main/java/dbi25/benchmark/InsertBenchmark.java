package dbi25.benchmark;

import java.sql.*;
import java.util.Date;

public class InsertBenchmark {
    static void insertBenchmark(int n, String server, String instance, String database, String user, String password) throws SQLException {
        // create Connection and PreparedStatement
        Connection conn = null;
        PreparedStatement preparedStatement = null;
        try {
            // connect to database
            final String dbUrl = "jdbc:sqlserver://" + server + "\\" + instance + ";databaseName=" + database;
            conn = DriverManager.getConnection(dbUrl, user, password);
            conn.setAutoCommit(false);
            System.out.println("\nConnected to Microsoft SQL Server!\n");

            // drop tables
            System.out.println("DROP TABLES");
            Statement statement = conn.createStatement();
            statement.executeUpdate("DROP TABLE if EXISTS history, tellers, accounts, branches;");

            // create tables
            System.out.println("CREATE NEW TABLES");
            statement = conn.createStatement();
            statement.executeUpdate("CREATE TABLE branches ( branchid INT NOT NULL, branchname CHAR(20) NOT NULL, balance INT NOT NULL, address CHAR(72) NOT NULL, PRIMARY KEY (branchid));" +
                    "CREATE TABLE accounts ( accid INT NOT NULL, name CHAR(20) NOT NULL, balance INT NOT NULL, branchid INT NOT NULL, address CHAR(68) NOT NULL, PRIMARY KEY (accid), FOREIGN KEY (branchid) REFERENCES branches );" +
                    "CREATE TABLE tellers ( tellerid INT NOT NULL, tellername CHAR(20) NOT NULL, balance INT NOT NULL, branchid INT NOT NULL, address CHAR(68) NOT NULL,  PRIMARY KEY (tellerid), FOREIGN KEY (branchid) REFERENCES branches );" +
                    "CREATE TABLE history ( accid INT NOT NULL, tellerid INT NOT NULL, delta INT NOT NULL, branchid INT NOT NULL, accbalance INT NOT NULL, cmmnt CHAR(30) NOT NULL, FOREIGN KEY (accid) REFERENCES accounts, FOREIGN KEY (tellerid) REFERENCES tellers, FOREIGN KEY (branchid) REFERENCES branches);");
            conn.commit();

            // save startTime
            Date startTime = new Date();

            // insert branches
            System.out.println("INSERT BRANCHES");
            preparedStatement = conn.prepareStatement("SET nocount ON INSERT INTO branches (branchid, branchname, balance, address) VALUES(?, '01234567890123456789', 0, '012345678901234567890123456789012345678901234567890123456789012345678901')");
            for (int i = 1; i <= n; i++) {
                preparedStatement.setInt(1, i);
                preparedStatement.addBatch();
            }
            preparedStatement.executeBatch();
            conn.commit();

            // insert accounts
            System.out.println("INSERT ACCOUNTS");
            Statement bulkStatement = conn.createStatement();
            bulkStatement.executeUpdate("ALTER TABLE accounts NOCHECK CONSTRAINT ALL\n" +
                    "SET NOCOUNT ON BULK INSERT accounts FROM 'C:\\accounts.csv' WITH(FORMAT='CSV', LASTROW= " + n * 100000 + ");\n" +
                    "ALTER TABLE accounts CHECK CONSTRAINT ALL"
            );
            conn.commit();

            // insert tellers
            System.out.println("INSERT TELLERS");
            preparedStatement = conn.prepareStatement("SET nocount ON INSERT INTO tellers (tellerid, tellername, balance, branchid, address) VALUES(?, '01234567890123456789', 0, ?, '01234567890123456789012345678901234567890123456789012345678901234567')");
            for (int i = 1; i <= n * 10; i++) {
                preparedStatement.setInt(1, i);
                preparedStatement.setInt(2, (int) (Math.random() * n) + 1);
                preparedStatement.addBatch();
            }
            preparedStatement.executeBatch();
            conn.commit();

            // calculate execution time
            Date endTime = new Date();
            long milliseconds = endTime.getTime() - startTime.getTime();
            System.out.println("FINISHED after " + milliseconds / 1000 + "s " + milliseconds % 1000 + "ms");
        } catch (SQLException e) {
            System.err.println(e.toString());
            System.exit(1);
        } finally {
            // close used resources
            if (preparedStatement != null) preparedStatement.close();
            if (conn != null) conn.close();
            System.out.println("\nDisconnected!\n");
        }
    }
}
