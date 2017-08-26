package com.iflytek.interact.db;

import com.mysql.jdbc.jdbc2.optional.MysqlDataSource;
import org.apache.commons.dbutils.QueryRunner;

import java.sql.Connection;
import java.sql.SQLException;

/**
 * @author rain
 */
public class DbUtil {
  private static MysqlDataSource dataSource = new MysqlDataSource();
  static QueryRunner run;

  static {
    dataSource.setURL("jdbc:mysql://172.16.59.13:10065/ifly_cpcc_bi_raiboo");
    dataSource.setUser("raiboo_biz");
    dataSource.setPassword("AxiaxxqEun8ZnROX");
    run = new QueryRunner(dataSource);
  }

  public static Connection getConnection() throws SQLException {
    return dataSource.getConnection();
  }

}
