package com.sree.dbBenchmark.impl;

import com.sree.dbBenchmark.PerfTest;
import com.sree.dbBenchmark.data.DagData;
import com.sree.dbBenchmark.data.PerfData;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.HttpSolrClient;
import org.apache.solr.common.SolrInputDocument;

import java.io.IOException;
import java.sql.SQLException;
import java.util.List;

public class SolrPerfTestImpl implements PerfTest{

  SolrClient solr = new HttpSolrClient.Builder("http://172.27.48.128:8983/solr/dag").build();

  private static SolrInputDocument createDoc(DagData dag) {
    SolrInputDocument document = new SolrInputDocument();

    document.addField("id", dag.dagID);
    document.addField("appID", dag.appID);
    document.addField("appAttemptID", dag.appAttemptID);
    document.addField("callerContextID", dag.callerContextID);

    document.addField("dagName", dag.dagName);
    document.addField("queueName", dag.queueName);

    document.addField("user", dag.user);
    document.addField("status", dag.status);
    document.addField("logURL", dag.logURL);
    document.addField("callerContextType", dag.callerContextType);

    document.addField("submitTime", dag.submitTime);
    document.addField("startTime", dag.startTime);
    document.addField("initTime", dag.initTime);
    document.addField("finishTime", dag.finishTime);
    document.addField("timeTaken", dag.timeTaken);

    document.addField("amWebServiceVersion", dag.amWebServiceVersion);
    document.addField("tezVersion", dag.tezVersion);

    document.addField("dagPlan", dag.dagPlan);
    document.addField("vertexNameIdMap", dag.vertexNameIdMap);
    document.addField("diagnostics", dag.diagnostics);
    document.addField("counters", dag.counters);
    document.addField("taskStats", dag.taskStats);

    return document;
  }

  public PerfData writeData(List<DagData> dataList) throws SQLException, SolrServerException, IOException {

    PerfData perfData = new PerfData();
    for(int i = 0; i < dataList.size(); i++) {
      solr.add(createDoc(dataList.get(i)));
    }
    perfData.registerEvent();
    solr.commit();
    perfData.registerEvent();

    return perfData;
  }

  public void readData(){

  }

}
