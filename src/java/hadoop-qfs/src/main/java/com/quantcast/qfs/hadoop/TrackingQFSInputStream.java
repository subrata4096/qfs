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
 * InputStream that wraps QFSInputStream to add optional counters and
 * file tracking
 */
package com.quantcast.qfs.hadoop;

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;

import com.quantcast.qfs.access.KfsAccess;

// Class that wraps normal QFSInputStream to track certain operations
public class TrackingQFSInputStream extends QFSInputStream {
  static final Log LOG = LogFactory.getLog(QFSInputStream.class);
  final String fileName;
  final IOStats readStats;
  final IOStats seekStats;
  final ElapsedStats closeStats;
  final boolean doCounters;
  final boolean doTracking;

  public TrackingQFSInputStream(KfsAccess kfsAccess, String path,
      FileSystem.Statistics stats, Configuration conf) throws IOException {
    super(kfsAccess, path, stats);
    fileName = path;
    readStats = new IOStats("Read");
    seekStats = new IOStats("Seek");
    closeStats = new ElapsedStats("Close");
    doCounters = conf.getBoolean("fs.qfs.optional.use.counters", false);
    doTracking = conf.getBoolean("fs.qfs.optional.track.files", false);
    if (doTracking) {
      LOG.info(fileName + " opened for read");
    }
  }

  @Override
  public synchronized int read() throws IOException {
    long startTime = System.nanoTime();
    int results = super.read();
    long elapsed =  System.nanoTime() - startTime;
    int bytes = (results >= 0) ? 1 : 0;
    if (doCounters) {
      OptionalCounters.Ops.Read.increment(elapsed, bytes);
    }
    if (doTracking) {
      readStats.addValue(elapsed, bytes);
    }
    return results;
  }

  @Override
  public synchronized int read(byte b[], int off, int len) throws IOException {
    long startTime = System.nanoTime();
    int results = super.read(b, off, len);
    long elapsed =  System.nanoTime() - startTime;
    int bytes = (results >= 0) ? results : 0;
    if (doCounters) {
      OptionalCounters.Ops.Read.increment(elapsed, bytes);
    }
    if (doTracking) {
      readStats.addValue(elapsed, bytes);
    }
    return results;
  }

  @Override
  public synchronized void seek(long targetPos) throws IOException {
    long startTime = System.nanoTime();
    long distance = Math.abs(super.getPos() - targetPos);
    super.seek(targetPos);
    long elapsed =  System.nanoTime() - startTime;
    if (doCounters) {
      OptionalCounters.Ops.Seek.increment(elapsed, distance);
    }
    if (doTracking) {
      seekStats.addValue(elapsed, distance);
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

  @Override
  public String toString() {
    return fileName + " " + readStats + " " + seekStats + " " + closeStats;
  }

}
