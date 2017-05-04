package com.sree.dbBenchmark;

import com.sree.dbBenchmark.data.DagData;
import org.apache.solr.client.solrj.SolrServerException;

import java.io.IOException;
import java.util.List;

public interface PerfTest {

  public void writeData(List<DagData> dataList) throws SolrServerException, IOException;
  public void readData();

}
