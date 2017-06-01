package com.sree.dbBenchmark;

import com.sree.dbBenchmark.data.DagData;
import com.sree.dbBenchmark.data.PerfData;
import org.apache.solr.client.solrj.SolrServerException;

import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

public class ConcurrentTests implements Runnable {

  Thread t;
  String name;
  int seedNum;
  int index;

  private static int threadCount = 0;
  private static Random random = new Random();

  List<PerfTest> perfTests;

  ConcurrentTests(int seed, int indx, List<PerfTest> pTests) {
    seedNum = seed;
    perfTests = pTests;
    index = indx;

    name = "name_" + threadCount;
    t = new Thread(this, name);
    t.start();
  }

  private void runReadTests() {
    try {

      int userID = 0;
      for (int i = 0; i < 10; i++) {
        Thread.sleep(random.nextInt(1000));

        userID = (i + seedNum) % 10;

        PerfData perfDataS = perfTests.get(0).readData(String.format("user:userName%d", userID), 10);
        PerfData perfDataP = perfTests.get(1).readData(String.format("userName = 'userName%d'", userID), 10);

        System.out.printf("%d, %d, %d, %d, %d, %d\n", i, index, perfDataS.getTotalDelay(), perfDataS.data, perfDataP.getTotalDelay(), perfDataP.data);

      }

    } catch (Exception e) {
      System.out.println(name + "Interrupted" + e.toString());
    }
  }

  private void runReadWriteTests() {
    try{

      int userID = 0;
      for (int i = 0; i < 10; i++) {
        Thread.sleep(random.nextInt(1000));

        userID = (i + seedNum) % 10;

        List<DagData> list = new ArrayList<DagData>();
        list.add(new DagData());

        PerfData perfDataS = perfTests.get(0).writeData(list);
        PerfData perfDataP = perfTests.get(1).writeData(list);

        System.out.printf("Write: 0, 0, 0, 0, 0, 0, %d, %d, %d, %d, %d, %d\n", i, index, perfDataS.getTotalDelay(), perfDataS.data, perfDataP.getTotalDelay(), perfDataP.data);


        perfDataS = perfTests.get(0).readData(String.format("user:userName%d", userID), 10);
        perfDataP = perfTests.get(1).readData(String.format("userName = 'userName%d'", userID), 10);

        System.out.printf("%d, %d, %d, %d, %d, %d, 0, 0, 0, 0, 0, 0\n", i, index, perfDataS.getTotalDelay(), perfDataS.data, perfDataP.getTotalDelay(), perfDataP.data);
      }

    } catch (Exception e) {
      System.out.println(name + "Interrupted" + e.toString());
    }
  }

  public void run() {

//    runReadTests();

    runReadWriteTests();

  }

}
