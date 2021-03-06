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

package com.gmarciani.flink_scaffolding.query3;

import com.gmarciani.flink_scaffolding.common.extractor.EventTimestampExtractor;
import com.gmarciani.flink_scaffolding.common.keyer.WordKeySelector;
import com.gmarciani.flink_scaffolding.common.sink.es.ESProperties;
import com.gmarciani.flink_scaffolding.common.sink.es.ESSink;
import com.gmarciani.flink_scaffolding.common.source.kafka.KafkaProperties;
import com.gmarciani.flink_scaffolding.query2.operator.TimedWordCounterAggregator;
import com.gmarciani.flink_scaffolding.query2.operator.TimedWordCounterWindowFunction;
import com.gmarciani.flink_scaffolding.query2.operator.WordRankerWindowFunction;
import com.gmarciani.flink_scaffolding.query2.tuple.TimedWord;
import com.gmarciani.flink_scaffolding.query2.tuple.WindowWordRanking;
import com.gmarciani.flink_scaffolding.query2.tuple.WindowWordWithCount;
import com.gmarciani.flink_scaffolding.query3.operator.MyESSinkFunction;
import com.gmarciani.flink_scaffolding.query3.operator.StoppableTimedWordKafkaSource;
import com.gmarciani.flink_scaffolding.query3.operator.TimedWordFilter;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.shaded.org.apache.curator.shaded.com.google.common.collect.Sets;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.nio.file.FileSystems;
import java.nio.file.Path;
import java.util.Set;
import java.util.concurrent.TimeUnit;

/**
 * The topology for query-3.
 * The application ranks words written to Kafka by their occurrences, within event time tumbling
 * windows.
 *
 * @author Giacomo Marciani {@literal <gmarciani@acm.org>}
 * @since 1.0
 */
public class TopologyQuery3 {

  /**
   * The program name.
   */
  public static final String PROGRAM_NAME = "query-3";

  /**
   * The program description.
   */
  public static final String PROGRAM_DESCRIPTION =
      "Ranks words written to netcat by their occurrences, within event time tumbling windows.";

  /**
   * The program main method.
   * @param args the command line arguments.
   */
  public static void main(String[] args) throws Exception {
    // CONFIGURATION
    ParameterTool parameter = ParameterTool.fromArgs(args);
    final String kafkaZookeeper = parameter.get("kafka.zookeeper", "localhost:2181");
    final String kafkaBootstrap = parameter.get("kafka.bootstrap", "localhost:9092");
    final String kafkaTopic = parameter.get("kafka.topic", "topic-query-3");
    final Path outputPath = FileSystems.getDefault().getPath(parameter.get("output", PROGRAM_NAME + ".out"));
    final String elasticsearch = parameter.get("elasticsearch", null);
    final long windowSize = parameter.getLong("windowSize", 10);
    final TimeUnit windowUnit = TimeUnit.valueOf(parameter.get("windowUnit", "SECONDS"));
    final int rankSize = parameter.getInt("rankSize", 3);
    final long tsEnd = parameter.getLong("tsEnd", 100000L);
    final Set<String> ignoredWords = Sets.newHashSet(parameter.get("ignoredWords", "")
        .trim().split(","));
    final int parallelism = parameter.getInt("parallelism", 1);

    // ENVIRONMENT
    final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
    env.setParallelism(parallelism);
    final KafkaProperties kafkaProps = new KafkaProperties(kafkaBootstrap, kafkaZookeeper);
    final ESProperties elasticsearchProps = ESProperties.fromPropString(elasticsearch);

    // CONFIGURATION RESUME
    System.out.println("############################################################################");
    System.out.printf("%s\n", PROGRAM_NAME);
    System.out.println("----------------------------------------------------------------------------");
    System.out.printf("%s\n", PROGRAM_DESCRIPTION);
    System.out.println("****************************************************************************");
    System.out.println("Kafka Zookeeper: " + kafkaZookeeper);
    System.out.println("Kafka Bootstrap: " + kafkaBootstrap);
    System.out.println("Kafka Topic: " + kafkaTopic);
    System.out.println("Output: " + outputPath);
    System.out.println("Elasticsearch: " + elasticsearch);
    System.out.println("Window: " + windowSize + " " + windowUnit);
    System.out.println("Rank Size: " + rankSize);
    System.out.println("Timestamp End: " + tsEnd);
    System.out.println("Ignored Words: " + ignoredWords);
    System.out.println("Parallelism: " + parallelism);
    System.out.println("############################################################################");

    // TOPOLOGY
    DataStream<TimedWord> timedWords = env.addSource(new StoppableTimedWordKafkaSource(kafkaTopic, kafkaProps, tsEnd));

    DataStream<TimedWord> fileterTimedWords = timedWords.filter(new TimedWordFilter(ignoredWords))
        .assignTimestampsAndWatermarks(new EventTimestampExtractor());

    DataStream<WindowWordWithCount> windowCounts = fileterTimedWords
        .keyBy(new WordKeySelector())
        .timeWindow(Time.of(windowSize, windowUnit))
        .aggregate(new TimedWordCounterAggregator(), new TimedWordCounterWindowFunction());

    DataStream<WindowWordRanking> ranking = windowCounts.timeWindowAll(Time.of(windowSize, windowUnit))
        .apply(new WordRankerWindowFunction(rankSize));

    ranking.writeAsText(outputPath.toAbsolutePath().toString(), FileSystem.WriteMode.OVERWRITE);

    if (elasticsearch != null) {
      ranking.addSink(new ESSink<>(elasticsearchProps,
          new MyESSinkFunction(elasticsearchProps.getIndexName(), elasticsearchProps.getTypeName()))
      );
    }

    // EXECUTION
    env.execute(PROGRAM_NAME);
  }
}
