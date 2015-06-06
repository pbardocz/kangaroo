/**
 * Copyright 2015 Conductor, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 *
 */
package com.conductor.s3;

import java.io.IOException;

import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileRecordReader;

/**
 * Copied directly from {@link org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat} (except the wrapper around
 * {@link #listStatus} which is not needed in this case), but inherits from the S3 optimized input format
 * {@link S3OptimizedFileInputFormat}.
 *
 * @author cgreen
 * @see S3OptimizedFileInputFormatMRV1
 */
public class S3SequenceFileInputFormat<K, V> extends S3OptimizedFileInputFormat<K, V> {

    @Override
    public RecordReader<K, V> createRecordReader(InputSplit split, TaskAttemptContext context) throws IOException {
        return new SequenceFileRecordReader<K, V>();
    }

    @Override
    protected long getFormatMinSplitSize() {
        return SequenceFile.SYNC_INTERVAL;
    }
}
