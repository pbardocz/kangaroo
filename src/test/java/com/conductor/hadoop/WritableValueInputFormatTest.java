package com.conductor.hadoop;

import static org.junit.Assert.assertEquals;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.*;

import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.io.SequenceFile.Reader;
import org.apache.hadoop.io.SequenceFile.Writer;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.junit.Test;

import com.google.common.collect.Lists;

/**
 * @author cgreen
 */
public class WritableValueInputFormatTest {

    private final FileSystem fs = mock(FileSystem.class);
    private final Path path = mock(Path.class);
    private final Configuration conf = new Configuration(false);

    @Test
    public void testSetupInput() throws Exception {
        final Text t1 = new Text("1");
        final Text t2 = new Text("2");
        final Text t3 = new Text("3");
        final Text t4 = new Text("4");
        final Text t5 = new Text("5");
        final List<Text> values = Lists.newArrayList(t1, t2, t3, t4, t5);

        final Writer writer = mock(Writer.class);
        when(path.getFileSystem(conf)).thenReturn(fs);
        final Job job = mock(Job.class);
        when(job.getConfiguration()).thenReturn(conf);
        final String fileName = "file:///tmp/file";
        when(path.toString()).thenReturn(fileName);

        WritableValueInputFormat.doSetupInput(values, Text.class, 2, job, path, writer);

        final NullWritable key = NullWritable.get();
        verify(writer, times(1)).append(key, t1);
        verify(writer, times(1)).append(key, t2);
        verify(writer, times(1)).append(key, t3);
        verify(writer, times(1)).append(key, t4);
        verify(writer, times(1)).append(key, t5);
        verify(writer, times(2)).sync();

        verify(writer).close();
        verify(writer).hflush();
        verify(fs).deleteOnExit(path);

        assertEquals(Text.class, conf.getClass(WritableValueInputFormat.VALUE_TYPE_CONF, NullWritable.class));
        assertEquals(2, conf.getInt(WritableValueInputFormat.INPUTS_PER_SPLIT_CONF, -1));
        assertEquals(fileName, conf.get(WritableValueInputFormat.INPUT_FILE_LOCATION_CONF));
    }

    @Test
    public void testGetSplits() throws Exception {
        final WritableValueInputFormat<Text> inputFormat = spy(new WritableValueInputFormat<Text>());

        final Reader reader = mock(Reader.class);
        doReturn(reader).when(inputFormat).getReader(conf, path);
        doReturn(fs).when(inputFormat).getFileSystem(conf);
        when(fs.makeQualified(any(Path.class))).thenReturn(path);
        when(fs.getFileStatus(path)).thenReturn(new FileStatus(100, false, 1, 10, 1, path));

        conf.set(WritableValueInputFormat.INPUT_FILE_LOCATION_CONF, "/tmp/input_file");
        conf.setInt(WritableValueInputFormat.INPUTS_PER_SPLIT_CONF, 2);
        conf.setClass(WritableValueInputFormat.VALUE_TYPE_CONF, Text.class, Writable.class);

        final JobContext jobCtx = mock(JobContext.class);
        when(jobCtx.getConfiguration()).thenReturn(conf);

        // 3 inputs
        when(reader.next(any(NullWritable.class), any(Text.class))).thenReturn(true).thenReturn(true).thenReturn(true)
                .thenReturn(false);

        // getPosition gets called after each for loop invocation, so only need two of these
        when(reader.getPosition()).thenReturn(30l).thenReturn(60l);

        final List<InputSplit> result = inputFormat.getSplits(jobCtx);

        assertEquals(2, result.size());
        final FileSplit fileSplit1 = (FileSplit) result.get(0);
        assertEquals(0, fileSplit1.getStart());
        assertEquals(30, fileSplit1.getLength());
        assertEquals(path, fileSplit1.getPath());

        final FileSplit fileSplit2 = (FileSplit) result.get(1);
        assertEquals(30, fileSplit2.getStart());
        assertEquals(70, fileSplit2.getLength());
        assertEquals(path, fileSplit2.getPath());
    }
}
