package Rec.dao;

import com.alibaba.druid.pool.DruidDataSourceFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.sql.DataSource;
import java.io.InputStream;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.Properties;

public class ExpoStatDaoImpl implements ExpoStatDao {
    private final Logger logger = LoggerFactory.getLogger(this.getClass());
    private static DataSource ds = null;

    static {
        try {
            InputStream in = ExpoStatDaoImpl.class.getClassLoader().getResourceAsStream("clickhouseds.properties");
            Properties props = new Properties();
            props.load(in);
            ds = DruidDataSourceFactory.createDataSource(props);
        }catch (Exception e){
            e.printStackTrace();
        }
    }

    @Override
    public Connection getConnection() throws SQLException {
        return ds.getConnection();
    }
}
