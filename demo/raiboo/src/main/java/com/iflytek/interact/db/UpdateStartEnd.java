package com.iflytek.interact.db;

import java.sql.SQLException;

import static com.iflytek.interact.db.DbUtil.run;

/**
 * @author rain
 */
public class UpdateStartEnd {

  public static void updateStart(String robotId) throws SQLException {
    run.update(DbUtil.getConnection(), "insert into raiboo_questionnaire_start_end (robot_id, action_name) " +
            "values (?, 'start')", robotId);
  }

  public static void updateEnd(String robotId) throws SQLException {
    run.update(DbUtil.getConnection(), "insert into raiboo_questionnaire_start_end (robot_id, action_name) " +
            "values (?, 'end')", robotId);
  }
}
