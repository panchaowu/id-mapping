package com.iflytek.raiboo.interact.db;

import java.sql.SQLException;

/**
 * @author rain
 */
public class UpdateStartEnd {

  public static void updateStart(String robotId) throws SQLException {
    DbUtil.run.update(DbUtil.getConnection(), "insert into raiboo_questionnaire_start_end (robot_id, action_name) " +
            "values (?, 'start')", robotId);
  }

  public static void updateEnd(String robotId) throws SQLException {
    DbUtil.run.update(DbUtil.getConnection(), "insert into raiboo_questionnaire_start_end (robot_id, action_name) " +
            "values (?, 'end')", robotId);
  }
}
