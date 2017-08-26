package com.iflytek.interact.bean;

import java.sql.Timestamp;

/**
 * @author rain
 */
public class StartEndBean {
  private String action;
  private Timestamp ts;

  public String getAction() {
    return action;
  }

  public void setAction(String action) {
    this.action = action;
  }

  public Timestamp getTs() {
    return ts;
  }

  public void setTs(Timestamp ts) {
    this.ts = ts;
  }
}
