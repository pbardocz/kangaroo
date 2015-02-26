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

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.LineRecordReader;

/**
 * Copied directly from {@link org.apache.hadoop.mapreduce.lib.input.TextInputFormat}, but inherits from the S3
 * optimized input format {@link S3OptimizedFileInputFormatMRV1}.
 *
 * @author cgreen
 * @see S3OptimizedFileInputFormatMRV1
 */
public class S3TextInputFormat extends S3OptimizedFileInputFormat<LongWritable, Text> {

    @Override
    public RecordReader<LongWritable, Text> createRecordReader(InputSplit split, TaskAttemptContext context) {
        final String delimiter = context.getConfiguration().get("textinputformat.record.delimiter");
        return new LineRecordReader(delimiter != null ? delimiter.getBytes() : null);
    }

    @Override
    protected boolean isSplitable(JobContext context, Path file) {
        final CompressionCodec codec = new CompressionCodecFactory(context.getConfiguration()).getCodec(file);
        return null == codec || codec instanceof SplittableCompressionCodec;
    }
}
