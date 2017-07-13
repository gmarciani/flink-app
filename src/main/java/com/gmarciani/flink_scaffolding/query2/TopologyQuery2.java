/*
  The MIT License (MIT)

  Copyright (c) 2017 Giacomo Marciani

  Permission is hereby granted, free of charge, to any person obtaining a copy
  of this software and associated documentation files (the "Software"), to deal
  in the Software without restriction, including without limitation the rights
  to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
  copies of the Software, and to permit persons to whom the Software is
  furnished to do so, subject to the following conditions:


  The above copyright notice and this permission notice shall be included in
  all copies or substantial portions of the Software.


  THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
  IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
  FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT.  IN NO EVENT SHALL THE
  AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
  LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
  OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
  THE SOFTWARE.
 */

package com.gmarciani.flink_scaffolding.query2;

import com.gmarciani.flink_scaffolding.common.extractor.EventTimestampExtractor;
import com.gmarciani.flink_scaffolding.common.keyer.EventKeyer;
import com.gmarciani.flink_scaffolding.query2.operator.StoppableTimedWordSocketSource;
import com.gmarciani.flink_scaffolding.query2.operator.*;
import com.gmarciani.flink_scaffolding.query2.tuple.TimedWord;
import com.gmarciani.flink_scaffolding.query2.tuple.WindowWordRanking;
import com.gmarciani.flink_scaffolding.query2.tuple.WindowWordWithCount;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.nio.file.FileSystems;
import java.nio.file.Path;
import java.util.concurrent.TimeUnit;

/**
 * The topology for query-2.
 * The application counts and ranks occurrences of words written to netcat, within event time tumbling
 * windows.
 *
 * @author Giacomo Marciani {@literal <gmarciani@acm.org>}
 * @since 1.0
 */
public class TopologyQuery2 {

  /**
   * The program name.
   */
  public static final String PROGRAM_NAME = "query-2";

  /**
   * The program description.
   */
  public static final String PROGRAM_DESCRIPTION =
      "Counts occurrences of words written to netcat, within event time tumbling windows.";

  /**
   * The program main method.
   * @param args the command line arguments.
   */
  public static void main(String[] args) throws Exception {
    // CONFIGURATION
    ParameterTool parameter = ParameterTool.fromArgs(args);
    final int port = Integer.valueOf(parameter.getRequired("port"));
    final Path outputPath = FileSystems.getDefault().getPath(parameter.get("output", PROGRAM_NAME + ".out"));
    final long windowSize = parameter.getLong("windowSize", 10);
    final TimeUnit windowUnit = TimeUnit.valueOf(parameter.get("windowUnit", "SECONDS"));
    final int rankSize = parameter.getInt("rankSize, 3");
    final int parallelism = parameter.getInt("parallelism", 1);

    // ENVIRONMENT
    final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
    env.setParallelism(parallelism);

    // CONFIGURATION RESUME
    System.out.println("############################################################################");
    System.out.printf("%s\n", PROGRAM_NAME);
    System.out.println("----------------------------------------------------------------------------");
    System.out.printf("%s\n", PROGRAM_DESCRIPTION);
    System.out.println("****************************************************************************");
    System.out.println("Port: " + port);
    System.out.println("Output: " + outputPath);
    System.out.println("Window: " + windowSize + " " + windowUnit);
    System.out.println("Rank Size: " + rankSize);
    System.out.println("Parallelism: " + parallelism);
    System.out.println("############################################################################");

    // TOPOLOGY
    DataStream<TimedWord> timedWords = env.addSource(new StoppableTimedWordSocketSource("localhost", port))
        .assignTimestampsAndWatermarks(new EventTimestampExtractor());

    DataStream<WindowWordWithCount> windowCounts = timedWords
        .keyBy(new EventKeyer())
        .timeWindow(Time.of(windowSize, windowUnit))
        .aggregate(new TimedWordCounterAggregator(), new TimedWordCounterWindowFunction());

    DataStream<WindowWordRanking> ranking = windowCounts.timeWindowAll(Time.of(windowSize, windowUnit))
        .apply(new WordRankerWindowFunction(rankSize));

    ranking.writeAsText(outputPath.toAbsolutePath().toString(), FileSystem.WriteMode.OVERWRITE);

    // EXECUTION
    env.execute(PROGRAM_NAME);
  }

}
