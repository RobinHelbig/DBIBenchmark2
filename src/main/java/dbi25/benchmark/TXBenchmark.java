package dbi25.benchmark;

import java.sql.*;
import java.util.Date;
import java.util.concurrent.ThreadLocalRandom;

import static java.sql.Connection.TRANSACTION_SERIALIZABLE;

public class TXBenchmark {
    private String dbUrl;
    private String user;
    private String password;
    private PreparedStatement checkBalanceStmt;
    private PreparedStatement analyseDeltaStmt;
    private CallableStatement depositStmt;


    TXBenchmark(String server, String instance, String database, String user, String password) {
        this.dbUrl = "jdbc:sqlserver://" + server + "\\" + instance + ";databaseName=" + database;
        this.user = user;
        this.password = password;
    }

    void startBenchmark() throws SQLException {
        //VOR DEM AUSFÜHREN EINMAL MANUELL "TRUNCATE TABLE HISTORY" AUSFÜHREN
        try (Connection conn = DriverManager.getConnection(dbUrl, user, password)) {
            conn.setTransactionIsolation(TRANSACTION_SERIALIZABLE);
            conn.setAutoCommit(false);
            prepareInsertion(conn);
            this.checkBalanceStmt = conn.prepareStatement("SELECT Balance FROM accounts WHERE accid = ?");
            this.depositStmt = conn.prepareCall("{ call dbo.updateBalance(?,?,?,?,?,?) }");
            this.analyseDeltaStmt = conn.prepareStatement("SELECT Count(1) AS deltaCount FROM history WHERE delta = ?");
            ThreadLocalRandom random = ThreadLocalRandom.current();
            System.out.println("STARTED!");
            Date startTime = new Date();
            int totalTransactions = 0;
            while (true) {
                int probability = random.nextInt(1, 101); //zwischen 1 und 100
                if (probability <= 35) {
                    int randomAccid = random.nextInt(1, 10000001); //zufällige accid
                    checkBalance(conn, randomAccid);
                } else if (probability <= 85) {
                    int randomTellerid = random.nextInt(1, 1001); //zufällige tellerid
                    int randomBranchid = random.nextInt(1, 101); //zufällige branchid
                    int randomAccid = random.nextInt(1, 10000001); //zufällige accid
                    int randomDelta = random.nextInt(1, 10001); //zufälliger Betrag zwischen 1 und 10000€
                    deposit(conn, randomAccid, randomTellerid, randomBranchid, randomDelta);
                } else {
                    int randomDelta = random.nextInt(1, 10001); //zufälliger Betrag zwischen 1 und 10000€
                    analyseDelta(conn, randomDelta);
                }

                Date nowTime = new Date();
                double minutes = (nowTime.getTime() - startTime.getTime()) / 60000.0; //Minuten seit Schleifenstart berechnen
                if (minutes >= 4 && minutes <= 9) { //Messphase
                    totalTransactions++;
                }
                if (minutes >= 10) { //Zeit vorbei
                    break;
                }

                Thread.sleep(50);
            }

            double averageTransactions = totalTransactions / (5.0 * 60);

            System.out.print("Total Transactions: " + totalTransactions + "\nAverage Transactions: " + averageTransactions);
        } catch (SQLException | InterruptedException e) {
            System.err.println(e.toString());
            System.exit(1);
        } finally {
            checkBalanceStmt.close();
            depositStmt.close();
            analyseDeltaStmt.close();
        }
        System.out.println("FINISHED!");
    }

    void prepareInsertion(Connection conn) throws SQLException {
        Statement stmt = conn.createStatement();
        stmt.executeUpdate(
                "CREATE OR ALTER PROCEDURE updateBalance\n" +
                        "@accid int,\n" +
                        "@branchid int,\n" +
                        "@tellerid int,\n" +
                        "@delta int,\n" +
                        "@cmnt nvarchar(30),\n" +
                        "@newBalance int output\n" +
                        "AS\n" +
                        "SET NOCOUNT ON;\n" +
                        "UPDATE accounts SET Balance = Balance + @delta WHERE accid = @accid\n" +
                        "SET @newBalance = (SELECT Balance FROM accounts WHERE accid = @accid)\n" +
                        "INSERT INTO History(accid, tellerid, delta, branchid, accbalance, cmmnt)\n" +
                        "VALUES(@accid,@tellerid,@delta,@branchid,@newBalance,@cmnt)" +
                        "UPDATE tellers SET Balance = Balance + @delta WHERE tellerid = @tellerid\n" +
                        "UPDATE branches SET Balance = Balance + @delta WHERE branchid = @branchid\n"
        );

        stmt.executeUpdate(
                "IF NOT EXISTS(\n" +
                        "SELECT 1 FROM sys.indexes WHERE name = 'deltaIndex'\n" +
                        ")\n" +
                        "BEGIN\n" +
                        "CREATE CLUSTERED INDEX deltaIndex ON [dbo].[history]\n" +
                        "(\n" +
                        "delta asc\n" +
                        ")\n" +
                        "END\n"
        );

        conn.commit();
    }

    int checkBalance(Connection conn, int accid) throws SQLException {
        ResultSet rs;

        checkBalanceStmt.setInt(1, accid);
        rs = checkBalanceStmt.executeQuery();
        conn.commit();

        rs.next();
        int balance = rs.getInt("Balance");
        return balance;
    }

    int deposit(Connection conn, int accId, int tellerId, int branchId, int delta) throws SQLException {
        int newAccountBalance;

        depositStmt.setInt(1, accId);
        depositStmt.setInt(2, branchId);
        depositStmt.setInt(3, tellerId);
        depositStmt.setInt(4, delta);
        depositStmt.setString(5, "123456789012345678901234567890");
        depositStmt.registerOutParameter(6, Types.INTEGER);
        depositStmt.execute();

        conn.commit();
        newAccountBalance = depositStmt.getInt(6);


        return newAccountBalance;
    }

    int analyseDelta(Connection conn, int delta) throws SQLException {
        ResultSet rs;

        analyseDeltaStmt.setInt(1, delta);
        rs = analyseDeltaStmt.executeQuery();
        rs.next();
        int deltaCount = rs.getInt("deltaCount");
        conn.commit();
        return deltaCount;
    }
}