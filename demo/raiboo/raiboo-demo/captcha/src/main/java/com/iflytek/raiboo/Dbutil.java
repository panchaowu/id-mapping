package com.iflytek.raiboo;

import com.mysql.jdbc.jdbc2.optional.MysqlDataSource;
import org.apache.commons.dbutils.QueryRunner;

import java.sql.SQLException;

/**
 * Created by jcao2014 on 2016/11/29.
 */
public class Dbutil {

    public MysqlDataSource dataSource = new MysqlDataSource();
    public QueryRunner run;

    public Dbutil() throws SQLException  {
        dataSource.setURL("jdbc:mysql://172.16.59.13:10065/ifly_cpcc_bi_raiboo");
        dataSource.setUser("raiboo_biz");
        dataSource.setPassword("AxiaxxqEun8ZnROX");
        run = new QueryRunner(dataSource);
    }
}
