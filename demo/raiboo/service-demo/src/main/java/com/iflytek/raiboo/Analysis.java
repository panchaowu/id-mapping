package com.iflytek.raiboo;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.iflytek.raiboo.interact.bean.AnalysisNode;
import com.iflytek.raiboo.interact.db.Statistics;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import java.util.List;

/**
 * @author rain
 */
@Path("analysis/{mid}")
public class Analysis {

  @GET
  @Produces(MediaType.TEXT_PLAIN)
  public static String getResult(@PathParam("mid") String mid) throws JsonProcessingException {
    List<AnalysisNode> ans = Statistics.statistics(mid);
    ObjectMapper mapper = new ObjectMapper();
    return mapper.writeValueAsString(ans);
  }

}
