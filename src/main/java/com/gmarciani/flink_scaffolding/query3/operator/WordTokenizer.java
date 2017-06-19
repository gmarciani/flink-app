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
import com.gmarciani.flink_scaffolding.query3.Query3;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.util.Collector;

/**
 * A simple words tokenizer.
 * Used in {@link Query3}.
 * @author Giacomo Marciani {@literal <gmarciani@acm.org>}
 * @since 1.0
 */
public class WordTokenizer implements FlatMapFunction<String,WordWithCount> {

  /**
   * The core method of the FlatMapFunction. Takes an element from the input data set and transforms
   * it into zero, one, or more elements.
   *
   * @param value The input value.
   * @param out   The collector for returning result values.
   * @throws Exception This method may throw exceptions. Throwing an exception will cause the operation
   *                   to fail and may trigger recovery.
   */
  @Override
  public void flatMap(String value, Collector<WordWithCount> out) throws Exception {
    for (String word : value.split("\\s")) {
      out.collect(new WordWithCount(word, 1L));
    }
  }
}
