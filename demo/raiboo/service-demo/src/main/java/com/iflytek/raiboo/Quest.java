package com.iflytek.raiboo;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.iflytek.raiboo.interact.bean.QuestionBean;
import com.iflytek.raiboo.interact.db.Questionnaire;
import com.iflytek.raiboo.interact.db.Smooth;
import com.iflytek.raiboo.interact.db.UpdateStartEnd;

import javax.ws.rs.Consumes;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import java.util.List;

/**
 * @author rain
 */
@Path("question")
public class Quest {

  @POST
  @Produces(MediaType.TEXT_PLAIN)
  @Consumes(MediaType.TEXT_PLAIN)
  public String getQuestions(String json) {
    ObjectMapper mapper = new ObjectMapper();
    JsonNode rootNode = null;
    try {
      rootNode = mapper.readTree(json);
      String robotId = rootNode.get("robot_id").asText();
      Questionnaire.Crowded crowd = Smooth.isCrowded(robotId);
      List<QuestionBean> qs = Questionnaire.questions(crowd);
      UpdateStartEnd.updateStart(robotId);
      return mapper.writeValueAsString(qs);
    } catch (Exception e) {
      e.printStackTrace();
    }
    return "empty";
  }
}
