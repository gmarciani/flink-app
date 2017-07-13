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
package com.gmarciani.flink_scaffolding.query2.tuple;

import com.gmarciani.flink_scaffolding.common.tuple.WordWithCount;
import lombok.AllArgsConstructor;
import lombok.Data;

/**
 * This class realizes a custom tuple for words.
 * Used in {@link com.gmarciani.flink_scaffolding.query2.TopologyQuery2}.
 *
 * @author Giacomo Marciani {@literal <gmarciani@acm.org>}
 * @since 1.0
 */
@Data
@AllArgsConstructor
public class TimedWord {

  /**
   * The event timestamp (ms).
   */
  public long timestamp;

  /**
   * The word.
   */
  public String word;

  public static TimedWord valueOf(String s) {
    String fields[] = s.split(",", -1);

    if (fields.length != 2) {
      throw new IllegalArgumentException("Malformed TimedWord: " + s);
    }

    final long timestamp = Long.valueOf(fields[0]);
    final String word = fields[1];

    return new TimedWord(timestamp,word);
  }
}
