package com.sree.dbBenchmark;

import com.sree.dbBenchmark.data.DagData;
import org.apache.solr.client.solrj.SolrServerException;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class Tests {

  private static int BATCH_COUNT = 1000;
  private static int BATCH_SIZE = 1000;

  public void writeTests(PerfTest perfTest) throws IOException, SolrServerException {
    for(int i = 0; i < BATCH_COUNT; i++) {
      List<DagData> list = new ArrayList<DagData>();
      for(int j = 0; j < BATCH_SIZE; j++) {
        list.add(new DagData());
      }

      long startTime = System.currentTimeMillis();
      perfTest.writeData(list);
      long timeElapsed = System.currentTimeMillis() - startTime;

      System.out.printf("Batch %d : %dms : %dW/S \n", (i + 1), timeElapsed, (BATCH_SIZE / (timeElapsed/1000)));
    }
  }

  public void readTests(PerfTest perfTest) {
  }

  public void readWriteTests(PerfTest perfTest) {
  }

}
