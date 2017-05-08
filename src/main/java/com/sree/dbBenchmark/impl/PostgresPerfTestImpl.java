package com.sree.dbBenchmark.impl;

import com.sree.dbBenchmark.PerfTest;
import com.sree.dbBenchmark.data.DagData;
import com.sree.dbBenchmark.data.PerfData;
import org.apache.solr.client.solrj.SolrServerException;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;

public class PostgresPerfTestImpl implements PerfTest{

  private Connection getConnection() {
    Connection postgres = null;
    try {
      Class.forName("org.postgresql.Driver");
      postgres = DriverManager.getConnection("jdbc:postgresql://172.27.48.128:5432/postgres", "postgres", "blah");
      postgres.setAutoCommit(false);
    } catch (Exception e) {
      e.printStackTrace();
      System.err.println(e.getClass().getName()+": "+e.getMessage());
      System.exit(0);
    }
    return postgres;
  }

  private static String createSql(DagData dag) {
    String sql = "INSERT INTO dag (" +

        "dagID, appID, appAttemptID, callerContextID, " +
        "dagName, queueName, " +
        "user, status, logURL, callerContextType, " +
        "submitTime, startTime, initTime, finishTime, timeTaken" +
        "amWebServiceVersion, tezVersion, " +
        "dagPlan, vertexNameIdMap, diagnostics, counters, taskStats)" +

        " VALUES (" +

        "'%s', '%s', '%s', '%s'," +
        "'%s', '%s'" +
        "'%s', '%s', '%s', '%s'" +
        "'%s', '%s', '%s', '%s'" +
        "'%s', '%s'" +
        "'%s', '%s', '%s', '%s', '%s')";

    return String.format(sql,
        dag.dagID,
        dag.appID,
        dag.appAttemptID,
        dag.callerContextID,

        dag.dagName,
        dag.queueName,

        dag.user,
        dag.status,
        dag.logURL,
        dag.callerContextType,

        dag.submitTime,
        dag.startTime,
        dag.initTime,
        dag.finishTime,
        dag.timeTaken,

        dag.amWebServiceVersion,
        dag.tezVersion,

        dag.dagPlan,
        dag.vertexNameIdMap,
        dag.diagnostics,
        dag.counters,
        dag.taskStats
        );
  }

  public PerfData writeData(List<DagData> dataList) throws SQLException, SolrServerException, IOException {

    Connection cn = getConnection();
    Statement stmt = cn.createStatement();

    PerfData perfData = new PerfData();
    for(int i = 0; i < dataList.size(); i++) {
      stmt.executeUpdate(createSql(dataList.get(i)));
    }
    perfData.registerEvent();
    stmt.close();
    cn.commit();
    cn.close();
    perfData.registerEvent();

    return perfData;

  }

  public void readData(){

  }

}
