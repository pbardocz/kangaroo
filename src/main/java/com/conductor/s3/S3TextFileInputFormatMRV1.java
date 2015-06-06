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

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.*;
import org.apache.hadoop.mapred.*;

/**
 * Copied directly from {@link org.apache.hadoop.mapred.TextInputFormat}, but inherits from the S3 optimized input
 * format {@link S3OptimizedFileInputFormatMRV1}.
 *
 * @author cgreen
 * @see S3OptimizedFileInputFormatMRV1
 */
public class S3TextFileInputFormatMRV1 extends S3OptimizedFileInputFormatMRV1<LongWritable, Text> implements
        JobConfigurable {
    private CompressionCodecFactory compressionCodecs = null;

    @Override
    public void configure(JobConf conf) {
        compressionCodecs = new CompressionCodecFactory(conf);
    }

    @Override
    protected boolean isSplitable(FileSystem fs, Path file) {
        final CompressionCodec codec = compressionCodecs.getCodec(file);
        return (null == codec) || (codec instanceof SplittableCompressionCodec);
    }

    @Override
    public RecordReader<LongWritable, Text> getRecordReader(InputSplit genericSplit, JobConf job, Reporter reporter)
            throws IOException {
        reporter.setStatus(genericSplit.toString());
        final String delimiter = job.get("textinputformat.record.delimiter");
        byte[] recordDelimiterBytes = null;
        if (null != delimiter) {
            recordDelimiterBytes = delimiter.getBytes();
        }
        return new LineRecordReader(job, (FileSplit) genericSplit, recordDelimiterBytes);
    }
}