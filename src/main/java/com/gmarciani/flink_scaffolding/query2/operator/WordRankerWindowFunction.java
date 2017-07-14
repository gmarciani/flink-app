/*
  The MIT License (MIT)

  Copyright (c) 2017 Giacomo Marciani and Michele Porretta

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
package com.gmarciani.flink_scaffolding.query2.operator;

import com.gmarciani.flink_scaffolding.query2.tuple.WindowWordRanking;
import com.gmarciani.flink_scaffolding.query2.tuple.WindowWordWithCount;
import lombok.Data;
import org.apache.flink.streaming.api.functions.windowing.AllWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

/**
 * The operator that calculates the global ranking of players by average speed.
 *
 * @author Giacomo Marciani {@literal <gmarciani@acm.org>}
 * @author Michele Porretta {@literal <mporretta@acm.org>}
 * @since 1.0
 */
@Data
public class WordRankerWindowFunction implements AllWindowFunction<WindowWordWithCount, WindowWordRanking, TimeWindow> {

  /**
   * The logger.
   */
  private static final Logger LOG = LoggerFactory.getLogger(WordRankerWindowFunction.class);

  /**
   * The size of the top-K ranking.
   */
  private int rankSize;

  /**
   * The output: ranking.
   */
  private WindowWordRanking ranking = new WindowWordRanking();

  /**
   * Creates a new {@link WordRankerWindowFunction} with the specified rank size.
   * @param rankSize the size of the top-k ranking.
   */
  public WordRankerWindowFunction(int rankSize) {
    this.rankSize = rankSize;
  }

  /**
   * Evaluates the window and outputs none or several elements.
   *
   * @param window The window that is being evaluated.
   * @param values The elements in the window being evaluated.
   * @param out    A collector for emitting elements.
   * @throws Exception The function may throw exceptions to fail the program and trigger recovery.
   */
  @Override
  public void apply(TimeWindow window, Iterable<WindowWordWithCount> values, Collector<WindowWordRanking> out) throws Exception {
    this.ranking.setWStart(window.getStart());
    this.ranking.setWStop(window.getEnd());

    List<WindowWordWithCount> rank = this.ranking.getRank();

    rank.clear();
    values.forEach(rank::add);
    rank.sort((e1,e2) -> Long.compare(e2.getCount(), e1.getCount()));
    int size = rank.size();
    rank.subList(Math.min(this.rankSize, size), size).clear();

    LOG.debug("WOUT: {}", this.ranking);

    out.collect(this.ranking);
  }
}
