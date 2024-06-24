import org.apache.commons.dbcp2.BasicDataSource;

import javax.sql.DataSource;
import java.io.IOException;
import java.io.InputStream;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.Properties;

public class Data_Source {

    private static final BasicDataSource dataSource = new BasicDataSource();

    static {
        Properties properties = new Properties();
        try (InputStream input = Data_Source.class.getClassLoader().getResourceAsStream("db.properties")) {
            if (input == null) {
                System.out.println("Sorry, unable to find db.properties");
            }
            // Load a properties file from class path, inside static block
            properties.load(input);
        } catch (IOException ex) {
            ex.printStackTrace();
        }

        dataSource.setDriverClassName(properties.getProperty("db.driverClassName"));
        dataSource.setUrl(properties.getProperty("db.url"));
        dataSource.setUsername(properties.getProperty("db.username"));
        dataSource.setPassword(properties.getProperty("db.password"));
        dataSource.setInitialSize(Integer.parseInt(properties.getProperty("db.initialSize")));
        dataSource.setMaxTotal(Integer.parseInt(properties.getProperty("db.maxTotal")));
        dataSource.setMinIdle(Integer.parseInt(properties.getProperty("db.minIdle")));
        dataSource.setMaxIdle(Integer.parseInt(properties.getProperty("db.maxIdle")));
        dataSource.setValidationQuery(properties.getProperty("db.validationQuery"));
        dataSource.setTestOnBorrow(true);
        dataSource.setTestWhileIdle(true);
        dataSource.setMaxWaitMillis(Long.parseLong(properties.getProperty("db.maxWaitMillis")));
        dataSource.setMinEvictableIdleTimeMillis(Long.parseLong(properties.getProperty("db.minEvictableIdleTimeMillis")));
        dataSource.setTimeBetweenEvictionRunsMillis(Long.parseLong(properties.getProperty("db.timeBetweenEvictionRunsMillis")));

        // Ensure that idle connections are evicted promptly
        dataSource.setRemoveAbandonedOnBorrow(true);
        dataSource.setRemoveAbandonedOnMaintenance(true);
        dataSource.setRemoveAbandonedTimeout(30); //  seconds
        dataSource.setLogAbandoned(true);
    }

    public static DataSource getDataSource() {
        return dataSource;
    }

    public static Connection getConnection() throws SQLException {
        return dataSource.getConnection();
    }

    public static synchronized void closeDataSource() throws SQLException {
        dataSource.close();
    }
}

