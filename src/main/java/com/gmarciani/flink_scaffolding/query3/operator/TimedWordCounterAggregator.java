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
package com.gmarciani.flink_scaffolding.query3.operator;

import com.gmarciani.flink_scaffolding.query3.tuple.TimedWord;
import com.gmarciani.flink_scaffolding.query3.tuple.WindowWordWithCount;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.java.tuple.Tuple1;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The operator that calculates players running statistics (with window).
 *
 * @author Giacomo Marciani {@literal <gmarciani@acm.org>}
 * @since 1.0
 */
public class TimedWordCounterAggregator implements AggregateFunction<TimedWord,Tuple1<Long>,WindowWordWithCount> {

  /**
   * The logger.
   */
  private static final Logger LOG = LoggerFactory.getLogger(TimedWordCounterAggregator.class);

  /**
   * Creates a new accumulator, starting a new aggregate.
   * <p>
   * <p>The new accumulator is typically meaningless unless a value is added
   * via {@link #add(Object, Object)}.
   * <p>
   * <p>The accumulator is the state of a running aggregation. When a program has multiple
   * aggregates in progress (such as per key and window), the state (per key and window)
   * is the size of the accumulator.
   *
   * @return A new accumulator, corresponding to an empty aggregate.
   */
  @Override
  public Tuple1<Long> createAccumulator() {
    return new Tuple1<>(0L);
  }

  /**
   * Adds the given value to the given accumulator.
   *
   * @param value       The value to add
   * @param accumulator The accumulator to add the value to
   */
  @Override
  public void add(TimedWord value, Tuple1<Long> accumulator) {
    accumulator.f0++;
    LOG.debug("IN ({}): {}", accumulator.f0, value);
  }

  /**
   * Gets the result of the aggregation from the accumulator.
   *
   * @param accumulator The accumulator of the aggregation
   * @return The final aggregation result.
   */
  @Override
  public WindowWordWithCount getResult(Tuple1<Long> accumulator) {
    return new WindowWordWithCount(0, 0, null, accumulator.f0);
  }

  /**
   * Merges two accumulators, returning an accumulator with the merged state.
   * <p>
   * <p>This function may reuse any of the given accumulators as the target for the merge
   * and return that. The assumption is that the given accumulators will not be used any
   * more after having been passed to this function.
   *
   * @param a An accumulator to merge
   * @param b Another accumulator to merge
   * @return The accumulator with the merged state
   */
  @Override
  public Tuple1<Long> merge(Tuple1<Long> a, Tuple1<Long> b) {
    return null;
  }
}
