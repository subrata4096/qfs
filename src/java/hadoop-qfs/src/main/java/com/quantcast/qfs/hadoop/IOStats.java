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
 * Keeps some statistics about bytes and elapsed time.
 */
package com.quantcast.qfs.hadoop;


// Class that maintains stats on elapsed time
class IOStats {
  final String name;
  long count;
  long bytesTotal;
  long bytesMax;
  long bytesMin;
  long elapsedTotal;
  long elapsedMax;
  long elapsedMin;

  IOStats(String name) {
    this.name = name;
  }

  // Adds additional data. Assumes elapsed time is in ns.
 void addValue(long elapsed, long bytes) {
    if (++count == 1) {
      bytesTotal = bytesMax = bytesMin = bytes;
      elapsedTotal = elapsedMax = elapsedMin = elapsed;
    }
    else {
      bytesTotal += bytes;
      bytesMin = (bytesMin > bytes) ? bytes : bytesMin;
      bytesMax = (bytesMax < bytes) ? bytes : bytesMax;
      elapsedTotal += elapsed;
      elapsedMin = (elapsedMin > elapsed) ? elapsed : elapsedMin;
      elapsedMax = (elapsedMax < elapsed) ? elapsed : elapsedMax;
    }
  }

  public String toString() {
    double elapsedTotalMS = elapsedTotal * ElapsedStats.NS_TO_MS;
    double elapsedMaxMS = elapsedMax * ElapsedStats.NS_TO_MS;
    double elapsedMinMS = elapsedMin * ElapsedStats.NS_TO_MS;
    double elapsedMeanMS = (count != 0) ? (elapsedTotalMS / count) : 0;
    double bytesMean = (count != 0) ? (((double) bytesTotal) / count) : 0;
    return String.format("%s count=%d bytes(total=%d min=%d max=%d mean=%.2f) elapsed ms(total=%.2f min=%.2f max=%.2f mean=%.2f)",
        name, count, bytesTotal, bytesMin, bytesMax, bytesMean, elapsedTotalMS, elapsedMinMS, elapsedMaxMS, elapsedMeanMS);
  }
}
