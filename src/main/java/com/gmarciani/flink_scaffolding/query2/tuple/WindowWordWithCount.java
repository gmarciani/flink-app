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

import com.gmarciani.flink_scaffolding.query1.TopologyQuery1;
import lombok.AllArgsConstructor;
import lombok.Data;

/**
 * A tuple for word counting.
 * Used in {@link TopologyQuery1}.
 * @author Giacomo Marciani {@literal <gmarciani@acm.org>}
 * @since 1.0
 */
@Data
@AllArgsConstructor
public class WindowWordWithCount {

  /**
   * The timestamp for the window start instant.
   */
  private long wStart;

  /**
   * The timestamp for the window stop instant.
   */
  private long wStop;

  /**
   * The word.
   */
  private String word;

  /**
   * The word occurrences.
   */
  private long count;

  /**
   * Empty constructor.
   */
  public WindowWordWithCount() { }

  /**
   * Creates a new tuple.
   * @param word the word.
   * @param count the word occurrences.
   */
  public WindowWordWithCount(String word, long count) {
    this.word = word;
    this.count = count;
  }

  @Override
  public String toString() {
    return String.format("%d,%d,%s,%d",
        this.wStart, this.wStop, this.word, this.count);
  }
}
