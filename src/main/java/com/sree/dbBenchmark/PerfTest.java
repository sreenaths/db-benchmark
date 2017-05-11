package com.sree.dbBenchmark;

import com.sree.dbBenchmark.data.DagData;
import com.sree.dbBenchmark.data.PerfData;
import org.apache.solr.client.solrj.SolrServerException;

import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public interface PerfTest {

  public PerfData writeData(List<DagData> dataList) throws SQLException, SolrServerException, IOException;

  public PerfData readData(String query, Integer rows) throws SQLException, IOException, SolrServerException;

  public PerfData getFacet() throws SQLException, IOException, SolrServerException;

  public void close() throws SQLException;
}
