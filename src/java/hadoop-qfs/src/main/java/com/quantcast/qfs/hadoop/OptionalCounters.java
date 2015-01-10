/**
 *
 * Licensed under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied. See the License for the specific language governing
 * permissions and limitations under the License.
 *
 * Stores the optional timing and additional data.
 */
package com.quantcast.qfs.hadoop;

import org.apache.hadoop.mapred.Counters;
import org.apache.hadoop.mapred.Counters.Counter;

public class OptionalCounters {
  enum Ops {
    FileType, Exists, DirEnum, DirIter, Stat, FullStat, Mkdir, Rename, Rmdir, Delete, FileInfo, Create, Open, Close, Seek, Read, Write, Flush;

    long count = 0;
    long prevCount = 0;
    Counter countCounter = null;
    long elapsed = 0;
    long prevElapsed = 0;
    Counter elapsedCounter = null;
    long value = 0;
    long prevValue = 0;
    Counter valueCounter = null;

    synchronized void increment(long elapsedIncr) {
      count++;
      elapsed += elapsedIncr;
    }

    synchronized void increment(long elapsedIncr, long valueIncr) {
      count++;
      elapsed += elapsedIncr;
      value += valueIncr;
    }

    synchronized void increment(long elapsedIncr, long valueIncr, long countIncr) {
      count += countIncr;
      elapsed += elapsedIncr;
      value += valueIncr;
    }

    synchronized void updateCounters(Counters counters) {
      long diff = count - prevCount;
      if (diff != 0) {
        if (countCounter == null) {
          countCounter = counters.findCounter("QFS", name() + "_Count");
        }
        countCounter.increment(diff);
        prevCount = count;
      }
      diff = elapsed - prevElapsed;
      if (diff != 0) {
        if (elapsedCounter == null) {
          elapsedCounter = counters.findCounter("QFS", name() + "_Elapsed");
        }
        elapsedCounter.increment(diff);
        prevElapsed = elapsed;
      }
      diff = value - prevValue;
      if (diff != 0) {
        if (valueCounter == null) {
          String extension = (this == Read || this == Write || this == Seek) ? "_Bytes" : "_Items";
          valueCounter = counters.findCounter("QFS", name() + extension);
        }
        valueCounter.increment(diff);
        prevValue = value;
      }
    }
  }

  /** Accumulate the data into Hadoop Counters */
  public static synchronized void updateCounters(Counters counters) {
    for (Ops op : Ops.values()) {
      op.updateCounters(counters);
    }
  }

}
