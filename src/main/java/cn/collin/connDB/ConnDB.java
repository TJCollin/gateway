package cn.collin.connDB;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

/**
 * Created by collin on 17-5-25.
 */
public class ConnDB {
    public static Connection getConn()
    {
        Connection conn = null;
        try
        {
            Class.forName("org.postgresql.Driver");
            String url = "jdbc:postgresql://localhost:5432/postgres";
            try
            {
                conn = DriverManager.getConnection(url, "postgres", "collin");
            }
            catch (SQLException e)
            {
                e.printStackTrace();
            }
        }
        catch (ClassNotFoundException e)
        {
            e.printStackTrace();
        }

        return conn;
    }
}
