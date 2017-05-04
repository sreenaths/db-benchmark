package com.sree.dbBenchmark;

import com.sree.dbBenchmark.data.DagData;
import com.sree.dbBenchmark.data.PerfData;
import org.apache.solr.client.solrj.SolrServerException;

import java.io.IOException;
import java.util.List;

public interface PerfTest {

  public PerfData writeData(List<DagData> dataList) throws SolrServerException, IOException;
  public void readData();

}
