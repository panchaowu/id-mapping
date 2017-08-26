package com.iflytek.raiboo.interact.bean;

import java.util.Map;

/**
 * @author rain
 */
public class AnalysisNode {
  private String name;
  private Integer count;
  private Map<String, Integer> data;

  public AnalysisNode(String name, Integer count, Map<String, Integer> data) {
    this.name = name;
    this.count = count;
    this.data = data;
  }

  public String getName() {
    return name;
  }

  public void setName(String name) {
    this.name = name;
  }

  public Integer getCount() {
    return count;
  }

  public void setCount(Integer count) {
    this.count = count;
  }

  public Map<String, Integer> getData() {
    return data;
  }

  public void setData(Map<String, Integer> data) {
    this.data = data;
  }
}
