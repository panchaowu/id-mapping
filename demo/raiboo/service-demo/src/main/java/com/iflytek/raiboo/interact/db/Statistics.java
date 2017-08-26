package com.iflytek.raiboo.interact.db;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.iflytek.raiboo.interact.bean.AnalysisNode;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * @author rain
 */
public class Statistics {

  public static List<AnalysisNode> statistics(String marketId) {
    Map<String, AnalysisNode> tempMap = Maps.newTreeMap();
    String normalQuery = String.format("select c.question_id, d.question, \n" +
            "case when e.content is null then c.option_id else e.content end as answer, \n" +
            "case when f.sess_times is null then 0 else f.sess_times end as uv\n" +
            "from (\n" +
            "select question_id, option_id\n" +
            "from `raiboo_questionnaire_q&a`\n" +
            "union all\n" +
            "select question_id, 'ALL'\n" +
            "from raiboo_questionnaire_quetion\n" +
            "union all\n" +
            "select 'ALL', 'ALL'\n" +
            "union all\n" +
            "select qid, option_id\n" +
            "from (select '5#4a' as qid union all select '5#4b') a\n" +
            "cross join (\n" +
            "select option_id\n" +
            "from `raiboo_questionnaire_q&a`\n" +
            "where question_id = '5'\n" +
            "union all\n" +
            "select 'ALL'\n" +
            ") b\n" +
            ") c\n" +
            "left join raiboo_questionnaire_quetion d on c.question_id = d.question_id\n" +
            "left join raiboo_questionnaire_option e on c.option_id = e.option_id\n" +
            "left join (\n" +
            "select question_id, answer_id, sess_times\n" +
            "from raiboo_questionnaire_analysis\n" +
            "where market_id = '%s'\n" +
            ") f\n" +
            "on c.question_id = f.question_id and c.option_id = f.answer_id " +
            "where c.question_id <> '4'", marketId);
    try {
      Statement statement = DbUtil.getConnection().createStatement();
      ResultSet rs = statement.executeQuery(normalQuery);
      while (rs.next()) {
        String qId = rs.getString(1);
        String question = rs.getString(2);
        String answer = rs.getString(3);
        Integer number = rs.getInt(4);
        if(qId.startsWith("5")){
          question = dealWithBrand(qId);
          if(question == null) {
            continue;
          }
        }
        if(qId.equals("ALL") && question == null) {
          question = qId;
        }
        AnalysisNode v = tempMap.getOrDefault(qId,
                new AnalysisNode(question, number, Maps.newHashMap()));
        v.getData().put(answer, number);
        tempMap.put(qId, v);
      }
      return tempMap.entrySet().stream().map(entry -> {
                AnalysisNode analysisNode = entry.getValue();
                analysisNode.setCount(analysisNode.getData().getOrDefault("ALL", 0));
                analysisNode.getData().remove("ALL");
                return analysisNode;
              }
      ).collect(Collectors.toList());
    } catch (SQLException e) {
      e.printStackTrace();
    }

    return Lists.newArrayList();
  }

  /**
   * deal special case
   * @param qId
   * @return needed statistics content
   */
  private static String dealWithBrand(String qId) {
    switch (qId) {
      case "5":
        return "品牌综合排名";
      case "5#4a":
        return "品牌忠诚用户";
      case "5#4b":
        return "知名品牌排名";
      default:
        return null;
    }
  }
}
