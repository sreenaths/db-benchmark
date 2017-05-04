package com.sree.dbBenchmark.data;

import java.util.ArrayList;
import java.util.List;

public class PerfData {

  private List<Long> events = new ArrayList<Long>();

  public PerfData() {
    registerEvent();
  }

  public int registerEvent() {
    events.add(System.currentTimeMillis());
    return events.size();
  }

  public long getEventDelay(int eventIndex) {
    return events.get(eventIndex) - events.get(eventIndex - 1);
  }

  public long getTotalDelay() {
    return events.get(events.size() - 1) - events.get(0);
  }
}
