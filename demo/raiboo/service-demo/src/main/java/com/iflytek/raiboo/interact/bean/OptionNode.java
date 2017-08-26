package com.iflytek.raiboo.interact.bean;

/**
 * @author rain
 */
public class OptionNode {
  private String id;
  private String content;
  private String voice;

//  public OptionNode(String id, String content, String voice) {
//    this.id = id;
//    this.content = content;
//    this.voice = voice;
//  }

  public String getId() {
    return id;
  }

  public void setId(String id) {
    this.id = id;
  }

  public String getContent() {
    return content;
  }

  public void setContent(String content) {
    this.content = content;
  }

  public String getVoice() {
    return voice;
  }

  public void setVoice(String voice) {
    this.voice = voice;
  }
}
