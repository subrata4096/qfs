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
 * Keeps some statistics about elapsed time.
 */
package com.quantcast.qfs.hadoop;

// Class that maintains stats on elapsed time
class ElapsedStats {
  static final double NS_TO_MS = 1.0 / 1000000.0;
  final String name;
  long count;
  long elapsedTotal;
  long elapsedMax;
  long elapsedMin;

  ElapsedStats(String name) {
    this.name = name;
  }

  // Adds additional data. Assumes elapsed time is in ns.
  void addValue(long elapsed) {
    if (++count == 1) {
      elapsedTotal = elapsedMax = elapsedMin = elapsed;
    }
    else {
      elapsedTotal += elapsed;
      elapsedMin = (elapsedMin > elapsed) ? elapsed : elapsedMin;
      elapsedMax = (elapsedMax < elapsed) ? elapsed : elapsedMax;
    }
  }

  public String toString() {
    double elapsedTotalMS = elapsedTotal * NS_TO_MS;
    double elapsedMaxMS = elapsedMax * NS_TO_MS;
    double elapsedMinMS = elapsedMin * NS_TO_MS;
    double elapsedMeanMS = (count != 0) ? (elapsedTotalMS / count) : 0;
    return String.format("%s count=%d elapsed ms(total=%.2f min=%.2f max=%.2f mean=%.2f)",
        name, count, elapsedTotalMS, elapsedMinMS, elapsedMaxMS, elapsedMeanMS);
  }
}
