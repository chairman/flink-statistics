package Rec.dao;

import java.sql.Connection;
import java.sql.SQLException;

public interface ExpoStatDao {
    public Connection getConnection() throws SQLException;
}
