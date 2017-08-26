package com.iflytek.raiboo.interact.bean;

import java.util.List;

/**
 * @author rain
 */
public class QuestionBean {
  private String type; // single or multi
  private QNode question;
  private List<OptionNode> optionNodes;

  public QuestionBean(String id, String content, String type, List<OptionNode> optionNodes) {
    this.type = type;
    this.question = new QNode(id, content);
    this.optionNodes = optionNodes;
  }

  // question node
  private class QNode {
    String id;
    String content;

    public QNode(String id, String content) {
      this.id = id;
      this.content = content;
    }

    public String getId() {
      return id;
    }

    public String getContent() {
      return content;
    }
  }

  public String getType() {
    return type;
  }

  public void setType(String type) {
    this.type = type;
  }

  public QNode getQuestion() {
    return question;
  }

  public void setQuestion(QNode question) {
    this.question = question;
  }

  public List<OptionNode> getOptionNodes() {
    return optionNodes;
  }

  public void setOptionNodes(List<OptionNode> optionNodes) {
    this.optionNodes = optionNodes;
  }
}
