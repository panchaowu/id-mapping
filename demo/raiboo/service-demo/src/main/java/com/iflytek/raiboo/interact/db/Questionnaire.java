package com.iflytek.raiboo.interact.db;

import com.google.common.collect.Sets;
import com.iflytek.raiboo.interact.bean.OptionNode;
import com.iflytek.raiboo.interact.bean.QuestionBean;
import org.apache.commons.dbutils.ResultSetHandler;
import org.apache.commons.dbutils.handlers.BeanListHandler;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * @author rain
 */
public class Questionnaire {

  private static Stream<Integer> generateQuestionIds(Crowded crowded) {
    List<Integer> brands = Arrays.asList(1, 2, 3, 4, 5, 6);
    List<Integer> consumes = Arrays.asList(7, 8, 9, 10, 11);
    List<Integer> popularize = Arrays.asList(12, 13);
    Collections.shuffle(brands);
    Collections.shuffle(consumes);
    Collections.shuffle(popularize);
    Set<Integer> ids = Sets.newHashSet();
    ids.add(brands.get(0));
    ids.add(consumes.get(0));
    ids.add(popularize.get(0));
    // not crowded
    if (crowded == Crowded.NOT) {
      ids.add(brands.get(1));
      ids.add(consumes.get(1));
    }
    if (ids.contains(4) || ids.contains(5)) {
      ids.addAll(Arrays.asList(4, 5));
    }
    return ids.stream();
  }

  public static List<OptionNode> getOptions(String qId) throws SQLException {
    ResultSetHandler<List<OptionNode>> h = new BeanListHandler<>(OptionNode.class);
    return DbUtil.run.query(DbUtil.getConnection(), "select a.option_id as id, b.content as content, b.voice_broadcast as voice " +
            " from `raiboo_questionnaire_q&a` a inner join raiboo_questionnaire_option b " +
            " on a.question_id = ? and a.option_id = b.option_id", h, qId);
  }

  private static QuestionBean id2Bean(String qId) {
    String query = String.format("select question_id, question, option_type from raiboo_questionnaire_quetion where question_id = '%s'", qId);
    try {
      Statement statement = DbUtil.getConnection().createStatement();
      ResultSet resultSet = statement.executeQuery(query);
      if (resultSet.next())
        return new QuestionBean(resultSet.getString(1), resultSet.getString(2),
                resultSet.getString(3), getOptions(qId));
    } catch (SQLException e) {
      e.printStackTrace();
    }
    return null;
  }

  // to generate questions
  public static List<QuestionBean> questions(Crowded crowded) {
    Stream<Integer> ids = generateQuestionIds(crowded);
    return ids.sorted()
            .map(id -> id2Bean(id.toString()))
            .collect(Collectors.toList());
  }


  public enum Crowded {
    IS, NOT
  }


}
