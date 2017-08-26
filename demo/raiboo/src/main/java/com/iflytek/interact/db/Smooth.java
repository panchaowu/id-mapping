package com.iflytek.interact.db;

import com.iflytek.interact.bean.StartEndBean;
import org.apache.commons.dbutils.ResultSetHandler;
import org.apache.commons.dbutils.handlers.BeanListHandler;

import java.sql.SQLException;
import java.util.List;

import static com.iflytek.interact.db.DbUtil.run;

/**
 * @author rain
 */
public class Smooth {

  private static Long CROWD_THRESHHOLD = 2 * 1000L;

  private static List<StartEndBean> recentActions(String robotId) {
    ResultSetHandler<List<StartEndBean>> h = new BeanListHandler<>(StartEndBean.class);
    try {
      return run.query(DbUtil.getConnection(), "select action_name as action, reg_time as ts " +
              "from raiboo_questionnaire_start_end " +
              "where robot_id = ? " +
              "order by reg_time desc limit 10", h, robotId);
    } catch (SQLException e) {
      e.printStackTrace();
    }
    return null;
  }

  public static Questionnaire.Crowded isCrowded(String robotId) {
    List<StartEndBean> actions = recentActions(robotId);
    if (actions == null || actions.size() < 8) {
      return Questionnaire.Crowded.NOT;
    }
    int counter = 0;
    for (int i = 0; i < actions.size() - 1; ) {
      // start - end
      if (actions.get(i).getAction().equals("start") &&
              actions.get(i + 1).getAction().equals("end")) {
        if (actions.get(i).getTs().getTime() - actions.get(i + 1).getTs().getTime() <= CROWD_THRESHHOLD)
          counter++;
        i += 2;
      } else {
        i++;
      }
    }
    if (counter == 4) {
      return Questionnaire.Crowded.IS;
    }
    return Questionnaire.Crowded.NOT;
  }
}
