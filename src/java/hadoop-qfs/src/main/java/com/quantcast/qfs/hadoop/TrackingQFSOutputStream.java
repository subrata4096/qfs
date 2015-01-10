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
 * OutputStream that wraps QFSOutputStream to add optional counters and
 * file tracking
 */
package com.quantcast.qfs.hadoop;

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;

import com.quantcast.qfs.access.KfsAccess;

// Class that wraps normal QFSOutputStream to track certain operations
public class TrackingQFSOutputStream extends QFSOutputStream {
  static final Log LOG = LogFactory.getLog(QFSOutputStream.class);
  final String fileName;
  final IOStats writeStats;
  final ElapsedStats flushStats;
  final ElapsedStats closeStats;
  final boolean doCounters;
  final boolean doTracking;

  public TrackingQFSOutputStream(KfsAccess kfsAccess, String path, short replication,
      boolean overwrite, boolean append, int mode, Configuration conf) throws IOException {
    super(kfsAccess, path, replication, overwrite, append, mode);
    fileName = path;
    writeStats = new IOStats("Write");
    flushStats = new ElapsedStats("Flush");
    closeStats = new ElapsedStats("Close");
    doCounters = conf.getBoolean("fs.qfs.optional.use.counters", false);
    doTracking = conf.getBoolean("fs.qfs.optional.track.files", false);
    if (doTracking) {
      LOG.info(fileName + " opened for write");
    }
  }

  @Override
  public void write(int in) throws IOException {
    long startTime = System.nanoTime();
    super.write(in);
    long elapsed =  System.nanoTime() - startTime;
    if (doCounters) {
      OptionalCounters.Ops.Write.increment(elapsed, 1);
    }
    if (doTracking) {
      writeStats.addValue(elapsed, 1);
    }
  }

  @Override
  public void write(byte b[]) throws IOException {
    long startTime = System.nanoTime();
    super.write(b);
    long elapsed =  System.nanoTime() - startTime;
    if (doCounters) {
      OptionalCounters.Ops.Write.increment(elapsed, b.length);
    }
    if (doTracking) {
      writeStats.addValue(elapsed, b.length);
    }
  }

  @Override
  public void write(byte b[], int off, int len) throws IOException {
    long startTime = System.nanoTime();
    super.write(b, off, len);
    long elapsed =  System.nanoTime() - startTime;
    if (doCounters) {
      OptionalCounters.Ops.Write.increment(elapsed, len);
    }
    if (doTracking) {
      writeStats.addValue(elapsed, len);
    }
  }

  @Override
  public void flush() throws IOException {
    long startTime = System.nanoTime();
    super.flush();
    long elapsed =  System.nanoTime() - startTime;
    if (doCounters) {
      OptionalCounters.Ops.Flush.increment(elapsed);
    }
    if (doTracking) {
      flushStats.addValue(elapsed);
    }
  }

  @Override
  public synchronized void close() throws IOException {
    long startTime = System.nanoTime();
    super.close();
    long elapsed =  System.nanoTime() - startTime;
    if (doCounters) {
      OptionalCounters.Ops.Close.increment(elapsed);
    }
    if (doTracking) {
      closeStats.addValue(elapsed);
      LOG.info("Closed " + toString());
    }
  }

  public String toString() {
    return fileName + ": " + writeStats + " " + flushStats + " " + closeStats;
  }

}
