package com.sree.dbBenchmark.data;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.Random;

public class DagData {

  // -- Indexed --
  // IDs
  public String dagID;
  public String appID;
  public String appAttemptID;
  public String callerContextID;

  // Names
  public String dagName;
  public String queueName;
  public String userName;

  public String status;

  public String[] tablesWritten;
  public String queryText;
  public boolean isDDL;

  // Time
  public Date submitTime;
  public Date startTime;
  public Date initTime;
  public Date finishTime;
  public long timeTaken;

  // -- Non Indexed --
  public String logURL;
  public String callerContextType;

  // Versions
  public String amWebServiceVersion;
  public String tezVersion;

  // JSON Blobs
  public String dagPlan;
  public String vertexNameIdMap;
  public String diagnostics;
  public String counters;
  public String taskStats;

  private static Random random = new Random();

  public static List<String> STATUSES = new ArrayList<String>(Arrays.asList("NEW", "INITED", "RUNNING", "SUCCEEDED", "FAILED", "KILLED", "ERROR", "TERMINATING", "COMMITTING"));

  private static String genNum(int length) {
   StringBuilder sb = new StringBuilder(length);
   for(int i=0; i < length; i++)
    sb.append((char)('0' + random.nextInt(10)));
   return sb.toString();
  }

  private String generateQuery() {
    ArrayList<String> sqlTemplates = new ArrayList<String>(Arrays.asList(
        "CREATE TABLE CUSTOMERS%s( ID   INT NOT NULL, NAME VARCHAR (20) NOT NULL, AGE  INT NOT NULL, ADDRESS CHAR (25) , SALARY DECIMAL (18, 2), PRIMARY KEY (ID));",
        "SELECT ID, NAME, SALARY FROM CUSTOMERS%s;",
        "SELECT ID, NAME, SALARY FROM CUSTOMERS%s WHERE SALARY > %s000;",
        "SELECT ID, NAME, SALARY FROM CUSTOMERS%s WHERE SALARY > %s000 AND age < %s;",
        "SELECT * FROM CUSTOMERS%s WHERE SALARY LIKE '20__';",
        "UPDATE CUSTOMERS%s SET ADDRESS = 'Pune' WHERE ID = %s;",
        "DELETE FROM CUSTOMERS%s WHERE ID = %s;",
        "SELECT * FROM CUSTOMERS%s WHERE ROWNUM <= %s;",
        "SELECT * FROM CUSTOMERS%s ORDER BY NAME, SALARY;",
        "SELECT NAME, SUM(SALARY) FROM CUSTOMERS%s GROUP BY NAME;",
        "SELECT DISTINCT SALARY FROM CUSTOMERS%s ORDER BY SALARY;"
    ));

    return String.format(sqlTemplates.get(random.nextInt(11)), genNum(1), genNum(1), genNum(1));
  }

  private String generateName() {
    int num = random.nextInt(5);
    if(num == 0) return "MrrSleep";
    else if(num == 1) return "mrrSleep";

    return String.format("dag_%s", genNum(10));
  }

  // This dag is not big enough. A single DAG JSON can go upto .2million characters or more
  public DagData() {
    long currentTime = System.currentTimeMillis();
    long timeTaken = random.nextInt(20 * 60 * 1000); // 0 to 20min

    this.dagID = String.format("dag_%s_%s_%s", genNum(12), genNum(4), genNum(2));
    this.appID = String.format("application_%s", genNum(1));
    this.appAttemptID = String.format("appattempt_%s_%s_%s", genNum(12), genNum(4), genNum(6));
    this.callerContextID = String.format("hive_%s_%s_%s-%s", genNum(14), genNum(8), genNum(4), genNum(4));

    this.dagName = generateName();
    this.queueName = String.format("queue%s", genNum(1));
    this.userName = String.format("userName%s", genNum(1));

    this.status = STATUSES.get(random.nextInt(9));

    this.tablesWritten = new String[2];
    this.tablesWritten[0] = String.format("table_%s", genNum(1));
    this.tablesWritten[1] = String.format("table_%s", genNum(1));
    this.queryText = generateQuery();
    this.isDDL = this.queryText.indexOf("CREATE") == 0;

    this.submitTime = new Date(currentTime);
    this.initTime = new Date(currentTime + 100);
    this.startTime = new Date(currentTime + 200);
    this.finishTime = new Date(currentTime + timeTaken);
    this.timeTaken = timeTaken;

    this.logURL = String.format("http://log.link/path/file_%s.log", genNum(5));
    this.callerContextType = "Hive";

    this.amWebServiceVersion = "2";
    this.tezVersion = "0.9.0";

//    this.dagPlan = genNum(2);
//    this.vertexNameIdMap = genNum(5);
//    this.diagnostics = genNum(1);
//    this.counters = genNum(5);
//    this.taskStats = genNum(5);

    this.dagPlan = genNum(20000);
    this.vertexNameIdMap = genNum(500);
    this.diagnostics = genNum(100);
    this.counters = genNum(5000);
    this.taskStats = genNum(500);
  }

}

