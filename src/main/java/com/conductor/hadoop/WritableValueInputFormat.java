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

package com.conductor.hadoop;

import static com.google.common.base.Preconditions.checkArgument;

import java.io.IOException;
import java.util.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.fs.Options.CreateOpts;
import org.apache.hadoop.io.*;
import org.apache.hadoop.io.SequenceFile.*;
import org.apache.hadoop.io.compress.DefaultCodec;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileRecordReader;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Strings;
import com.google.common.collect.Lists;

/**
 * An {@link InputFormat} that writes any number of {@link Writable} values to a {@link SequenceFile} as inputs to a
 * Map/Reduce job, and allows the user to specify how many of these inputs go into each {@link InputSplit}.
 * <p/>
 * This is useful when you are using Map/Reduce to distribute work across many machines; define the unit of work in
 * {@link V} and the number of {@link V}s to process per map task, and do work on {@link V} in your
 * {@linkplain Mapper#map map} method.
 *
 * @author cgreen
 */
public class WritableValueInputFormat<V extends Writable> extends InputFormat<NullWritable, V> {

    public static final String INPUT_FILE_LOCATION_CONF = "input.values.file.location";
    public static final String INPUTS_PER_SPLIT_CONF = "input.values.per.split";
    public static final String VALUE_TYPE_CONF = "value.type";
    private static final int DEFAULT_INPUTS_PER_SPLIT = 20;
    @VisibleForTesting
    static final DefaultCodec CODEC = new DefaultCodec();
    @VisibleForTesting
    static final CreateOpts[] DUMMY_VAR_ARGS = new CreateOpts[] {};

    @Override
    public List<InputSplit> getSplits(final JobContext context) throws IOException, InterruptedException {
        final Configuration conf = context.getConfiguration();

        // init the reader
        final String filePath = conf.get(INPUT_FILE_LOCATION_CONF);
        checkArgument(!Strings.isNullOrEmpty(filePath), "Missing property: " + INPUT_FILE_LOCATION_CONF);

        final FileSystem fs = getFileSystem(conf);
        final Path path = fs.makeQualified(new Path(filePath));
        final SequenceFile.Reader reader = getReader(conf, path);

        // create the splits by looping through the values of the input file
        int totalInputs = 0;
        int maxInputsPerSplit = conf.getInt(INPUTS_PER_SPLIT_CONF, DEFAULT_INPUTS_PER_SPLIT);
        long pos = 0L;
        long last = 0L;
        long lengthRemaining = fs.getFileStatus(path).getLen();
        final List<InputSplit> splits = Lists.newArrayList();
        final V value = getV(conf);
        for (final NullWritable key = NullWritable.get(); reader.next(key, value); last = reader.getPosition()) {
            if (++totalInputs % maxInputsPerSplit == 0) {
                long splitSize = last - pos;
                splits.add(new FileSplit(path, pos, splitSize, null));
                lengthRemaining -= splitSize;
                pos = last;
            }
        }
        // create the last split if there is data remaining
        if (lengthRemaining != 0) {
            splits.add(new FileSplit(path, pos, lengthRemaining, null));
        }
        return splits;
    }

    @VisibleForTesting
    Reader getReader(final Configuration conf, final Path path) throws IOException {
        return new Reader(conf, Reader.file(path));
    }

    @VisibleForTesting
    FileSystem getFileSystem(final Configuration conf) throws IOException {
        return FileSystem.get(conf);
    }

    @SuppressWarnings("unchecked")
    private V getV(final Configuration conf) {
        final Class<? extends Writable> valueClass = conf.getClass(VALUE_TYPE_CONF, null, Writable.class);
        try {
            // note that, since valueClass is Writable, it should have a default constructor.
            return (V) valueClass.newInstance();
        } catch (InstantiationException e) {
            throw new RuntimeException(e);
        } catch (IllegalAccessException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public RecordReader<NullWritable, V> createRecordReader(final InputSplit split, final TaskAttemptContext context)
            throws IOException, InterruptedException {
        final SequenceFileRecordReader<NullWritable, V> reader = new SequenceFileRecordReader<NullWritable, V>();
        reader.initialize(split, context);
        return reader;
    }

    /**
     * Writes the provided {@code values} to an input file to be read by the {@link Job}, and sets up all additional
     * necessary configuration.
     * 
     * @param values
     *            the values to be read by the job.
     * @param clazz
     *            the type of the values.
     * @param inputsPerSplit
     *            how man inputs each split gets
     * @param job
     *            the job to configure
     * @param <V>
     *            the type of the {@code values}
     * @throws IOException
     */
    public static <V extends Writable> void setupInput(final List<V> values, Class<V> clazz, final int inputsPerSplit,
            final Job job) throws IOException {
        final Path inputPath = new Path("job_input_" + System.currentTimeMillis() + UUID.randomUUID().toString());
        final Writer writer = SequenceFile.createWriter(FileContext.getFileContext(job.getConfiguration()),
                job.getConfiguration(), inputPath, NullWritable.class, clazz, CompressionType.NONE, CODEC,
                new Metadata(), EnumSet.of(CreateFlag.CREATE), DUMMY_VAR_ARGS);
        doSetupInput(values, clazz, inputsPerSplit, job, inputPath, writer);
    }

    @VisibleForTesting
    static <V extends Writable> void doSetupInput(final List<V> values, final Class<V> clazz, final int inputsPerSplit,
            final Job job, final Path inputPath, final Writer writer) throws IOException {
        job.getConfiguration().setClass(VALUE_TYPE_CONF, clazz, Writable.class);
        job.getConfiguration().setInt(INPUTS_PER_SPLIT_CONF, inputsPerSplit);
        job.getConfiguration().set(INPUT_FILE_LOCATION_CONF, inputPath.toString());

        // write each value to the sequence file
        int syncCounter = 0;
        for (final V input : values) {
            // each entry in the sequence file is a map input
            writer.append(NullWritable.get(), input);
            // syncing indicates an input split boundary
            if (++syncCounter % inputsPerSplit == 0) {
                writer.sync();
            }
        }
        // close the input file
        writer.hflush();
        writer.close();

        // delete file when JVM exits
        inputPath.getFileSystem(job.getConfiguration()).deleteOnExit(inputPath);
    }
}
