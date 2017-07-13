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
package com.gmarciani.flink_scaffolding.query3.operator;

import com.gmarciani.flink_scaffolding.common.tuple.WordWithCount;
import com.gmarciani.flink_scaffolding.query2.tuple.TimedWord;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010;
import org.apache.flink.streaming.util.serialization.AbstractDeserializationSchema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Properties;

/**
 * A source that produces {@link WordWithCount} from a Kafka topic.
 * @author Giacomo Marciani {@literal <gmarciani@acm.org>}
 * @since 1.0
 */
public class StoppableTimedWordKafkaSource extends FlinkKafkaConsumer010<TimedWord> {

  /**
   * Constructs a new Kafka source parsing lines.
   * @param topic Kafka topics.
   * @param props Kafka properties.
   */
  public StoppableTimedWordKafkaSource(String topic, Properties props) {
    super(topic, new TimedWordDeserializationSchema(), props);
  }

  /**
   * The Kafka deserialization schema for {@link TimedWord}.
   * @author Giacomo Marciani {@literal <gmarciani@acm.org>}
   * @since 1.0
   */
  public static final class TimedWordDeserializationSchema extends AbstractDeserializationSchema<TimedWord> {

    /**
     * The logger.
     */
    private static final Logger LOG = LoggerFactory.getLogger(TimedWordDeserializationSchema.class);

    /**
     * De-serializes the byte message.
     *
     * @param message The message, as a byte array.
     * @return The de-serialized message as an object.
     */
    @Override
    public TimedWord deserialize(byte[] message) throws IOException {
      TimedWord tuple = null;

      try {
        tuple = TimedWord.valueOf(new String(message));
      } catch (IllegalArgumentException exc) {
        LOG.warn(exc.getMessage());
      }

      return tuple;
    }

    @Override
    public boolean isEndOfStream(TimedWord tuple) {
      return (tuple != null) && tuple.getTimestamp() == Long.MAX_VALUE;
    }
  }
}
