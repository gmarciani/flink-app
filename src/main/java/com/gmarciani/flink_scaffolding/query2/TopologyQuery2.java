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

import com.gmarciani.flink_scaffolding.common.source.kafka.KafkaProperties;
import com.gmarciani.flink_scaffolding.common.source.kafka.LineKafkaSource;
import com.gmarciani.flink_scaffolding.query2.operator.WordCountReducer;
import com.gmarciani.flink_scaffolding.query2.operator.WordTokenizer;
import com.gmarciani.flink_scaffolding.common.tuple.WordWithCount;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;

/**
 * The topology for query-2.
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
  public static final String PROGRAM_DESCRIPTION = "Counts occurrences of words written to Kafka, within time window.";

  /**
   * The program main method.
   * @param args the command line arguments.
   */
  public static void main(String[] args) throws Exception {

    // CONFIGURATION
    ParameterTool parameter = ParameterTool.fromArgs(args);
    final String kafkaZookeeper = parameter.get("kafka.zookeeper", "localhost:2181");
    final String kafkaBootstrap = parameter.get("kafka.bootstrap", "localhost:9092");
    final String kafkaTopic = parameter.get("kafka.topic", "sample-topic-query-2");
    final int parallelism = parameter.getInt("parallelism", 1);

    // ENVIRONMENT
    final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.setParallelism(parallelism);

    // CONFIGURATION RESUME
    System.out.println("############################################################################");
    System.out.printf("%s\n", PROGRAM_NAME);
    System.out.println("----------------------------------------------------------------------------");
    System.out.printf("%s\n", PROGRAM_DESCRIPTION);
    System.out.println("****************************************************************************");
    System.out.println("Kafka Zookeeper: " + kafkaZookeeper);
    System.out.println("Kafka Bootstrap: " + kafkaBootstrap);
    System.out.println("Kafka Topic: " + kafkaTopic);
    System.out.println("Parallelism: " + parallelism);
    System.out.println("############################################################################");

    // TOPOLOGY
    KafkaProperties kafkaProps = new KafkaProperties(kafkaBootstrap, kafkaZookeeper);
    DataStream<String> text = env.addSource(new LineKafkaSource(kafkaTopic, kafkaProps));

    DataStream<WordWithCount> windowCounts = text
        .flatMap(new WordTokenizer())
        .keyBy("word")
        .timeWindow(Time.seconds(5), Time.seconds(1))
        .reduce(new WordCountReducer());

    windowCounts.print().setParallelism(1);

    // EXECUTION
    env.execute(PROGRAM_NAME);
  }

}
