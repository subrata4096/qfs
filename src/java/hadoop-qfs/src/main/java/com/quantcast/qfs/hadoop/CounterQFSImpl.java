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
 * Implements a version of QFSImpl that wraps some QFSImpl methods to time
 * some methods of interest. The timing info and some additional data depending
 * on the method are accumulated in OptionalCounters.
 *
 * This mode is activated if the Configuration used when creating the QFS instance
 * has the property "fs.qfs.optional.use.counters" defined and set to true.
 */

package com.quantcast.qfs.hadoop;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import com.quantcast.qfs.access.KfsFileAttr;

public class CounterQFSImpl extends QFSImpl {

  public CounterQFSImpl(String metaServerHost, int metaServerPort,
      FileSystem.Statistics stats, Configuration conf) throws IOException {
    super(metaServerHost, metaServerPort, stats, conf);
  }

  @Override
  public boolean exists(String path) throws IOException {
    long startTime = System.nanoTime();
    boolean ret = super.exists(path);
    OptionalCounters.Ops.Exists.increment(System.nanoTime() - startTime);
    return ret;
  }

  @Override
  public boolean isDirectory(String path) throws IOException {
    long startTime = System.nanoTime();
    boolean ret = super.isDirectory(path);
    OptionalCounters.Ops.FileType.increment(System.nanoTime() - startTime);
    return ret;
  }

  @Override
  public boolean isFile(String path) throws IOException {
    long startTime = System.nanoTime();
    boolean ret = super.isFile(path);
    OptionalCounters.Ops.FileType.increment(System.nanoTime() - startTime);
    return ret;
  }

  @Override
  public FileStatus[] readdirplus(Path path) throws IOException {
    long startTime = System.nanoTime();
    FileStatus[] ret = super.readdirplus(path);
    long value = (ret != null) ? ret.length : 0;
    OptionalCounters.Ops.DirEnum.increment(System.nanoTime() - startTime, value);
    return ret;
  }

  @Override
  public FileStatus stat(Path path) throws IOException {
    long startTime = System.nanoTime();
    FileStatus ret = super.stat(path);
    OptionalCounters.Ops.Stat.increment(System.nanoTime() - startTime);
    return ret;
  }

  @Override
  public KfsFileAttr fullStat(Path path) throws IOException {
    long startTime = System.nanoTime();
    KfsFileAttr ret = super.fullStat(path);
    OptionalCounters.Ops.FullStat.increment(System.nanoTime() - startTime);
    return ret;
  }

  @Override
  public int mkdirs(String path, int mode) throws IOException {
    long startTime = System.nanoTime();
    int ret = super.mkdirs(path, mode);
    OptionalCounters.Ops.Mkdir.increment(System.nanoTime() - startTime);
    return ret;
  }

  @Override
  public int mkdir(String path, int mode) throws IOException {
    long startTime = System.nanoTime();
    int ret = super.mkdir(path, mode);
    OptionalCounters.Ops.Mkdir.increment(System.nanoTime() - startTime);
    return ret;
  }

  @Override
  public int rename2(String source, String dest, boolean overwrite)
      throws IOException {
    long startTime = System.nanoTime();
    int ret = super.rename2(source, dest, overwrite);
    OptionalCounters.Ops.Rename.increment(System.nanoTime() - startTime);
    return ret;
  }

  @Override
  public int rename(String source, String dest) throws IOException {
    long startTime = System.nanoTime();
    int ret = super.rename(source, dest);
    OptionalCounters.Ops.Rename.increment(System.nanoTime() - startTime);
    return ret;
  }

  @Override
  public int rmdir(String path) throws IOException {
    long startTime = System.nanoTime();
    int ret = super.rmdir(path);
    OptionalCounters.Ops.Rmdir.increment(System.nanoTime() - startTime);
    return ret;
  }

  @Override
  public int rmdirs(String path) throws IOException {
    long startTime = System.nanoTime();
    int ret = super.rmdirs(path);
    OptionalCounters.Ops.Rmdir.increment(System.nanoTime() - startTime);
    return ret;
  }

  @Override
  public int remove(String path) throws IOException {
    long startTime = System.nanoTime();
    int ret = super.remove(path);
    OptionalCounters.Ops.Delete.increment(System.nanoTime() - startTime);
    return ret;
  }

  @Override
  public long filesize(String path) throws IOException {
    long startTime = System.nanoTime();
    long ret = super.filesize(path);
    OptionalCounters.Ops.FileInfo.increment(System.nanoTime() - startTime);
    return ret;
  }

  @Override
  public long getModificationTime(String path) throws IOException {
    long startTime = System.nanoTime();
    long ret = super.filesize(path);
    OptionalCounters.Ops.FileInfo.increment(System.nanoTime() - startTime);
    return ret;
  }

  public FSDataOutputStream create(String path, short replication,
      int bufferSize, boolean overwrite, int mode,
      boolean append) throws IOException {
    return new FSDataOutputStream(createQFSOutputStream(
        kfsAccess, path, replication, overwrite, append, mode), statistics);
  }

  public FSDataInputStream open(String path, int bufferSize)
      throws IOException {
    return new FSDataInputStream(createQFSInputStream(kfsAccess, path,
        statistics));
  }

  public CloseableIterator<FileStatus> getFileStatusIterator(FileSystem fs, Path path)
      throws IOException {
    return new CounterKfsFileStatusIterator(fs, path);
  }

  // Iterator returning each directory entry as a FileStatus
  public class CounterKfsFileStatusIterator extends KfsFileStatusIterator {

    public CounterKfsFileStatusIterator(FileSystem fs, Path p) throws IOException {
      super(fs, p);
      OptionalCounters.Ops.DirIter.increment(0, 0, 1);
    }

    @Override
    public FileStatus next() {
      long startTime = System.nanoTime();
      FileStatus ret = super.next();
      OptionalCounters.Ops.DirIter.increment(System.nanoTime() - startTime, 1, 0);
      return ret;
    }
  }

}
