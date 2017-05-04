package com.sree.dbBenchmark.data;

import java.util.Date;
import java.util.Random;

public class DagData {

  // IDs
  public String dagID;
  public String appID;
  public String appAttemptID;
  public String callerContextID;

  // Names
  public String dagName;
  public String queueName;

  // Others
  public String user;
  public String status;
  public String logURL; //NI
  public String callerContextType;

  // Time
  public Date submitTime;
  public Date startTime;
  public Date initTime;
  public Date finishTime;
  public int timeTaken;

  // Versions
  public String amWebServiceVersion; //NI
  public String tezVersion; //NI

  // JSON Blobs - NI
  public String dagPlan;
  public String vertexNameIdMap;
  public String diagnostics;
  public String counters;
  public String taskStats;

  private static Random random = new Random();

  public static String genNum(int length) {
   StringBuilder sb = new StringBuilder(length);
   for(int i=0; i < length; i++)
    sb.append((char)('0' + random.nextInt(10)));
   return sb.toString();
  }

  public DagData() {
    this.dagID = String.format("dag_%s_%s_%s", genNum(12), genNum(4), genNum(2));
    this.appID = String.format("application_%s_%s", genNum(12), genNum(4));
    this.appAttemptID = String.format("appattempt_%s_%s_%s", genNum(12), genNum(4), genNum(6));
    this.callerContextID = String.format("hive_%s_%s_%s-%s", genNum(14), genNum(8), genNum(4), genNum(4));

    this.dagName = String.format("dag_%s", genNum(10));
    this.queueName = String.format("queue%s", genNum(1));

    this.user = String.format("user%s", genNum(1));
    this.status = String.format("status%s", genNum(1));
    this.logURL = String.format("http://log.link/path/file_%s.log", genNum(5));
    this.callerContextType = "Hive";

    this.submitTime = new Date();
    this.startTime = new Date();
    this.initTime = new Date();
    this.finishTime = new Date();
    this.timeTaken = random.nextInt(1000);

    this.amWebServiceVersion = "2";
    this.tezVersion = "0.9.0";

    this.dagPlan = genNum(20000);
    this.vertexNameIdMap = genNum(500);
    this.diagnostics = genNum(100);
    this.counters = genNum(500);
    this.taskStats = genNum(500);
  }

}
