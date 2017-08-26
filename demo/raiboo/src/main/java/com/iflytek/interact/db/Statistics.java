package com.iflytek.interact.db;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.iflytek.interact.bean.AnalysisNode;

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
    String normalQuery = String.format("select a.question_id, b.question, " +
            "case when c.content is null then a.answer_id else c.content end as answer, a.sess_times\n" +
            "from raiboo_questionnaire_analysis a\n" +
            "left join raiboo_questionnaire_quetion b on a.question_id = b.question_id\n" +
            "left join raiboo_questionnaire_option c on a.answer_id = c.option_id\n" +
            "where a.question_id <> '4' and market_id = '%s'", marketId);
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
