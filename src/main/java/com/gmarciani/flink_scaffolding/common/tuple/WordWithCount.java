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
package com.gmarciani.flink_scaffolding.common.tuple;

/**
 * A tuple for word counting.
 * Used in {@link com.gmarciani.flink_scaffolding.query1.Query1}.
 * @author Giacomo Marciani {@literal <gmarciani@acm.org>}
 * @since 1.0
 */
public class WordWithCount {

  /**
   * The word.
   */
  public String word;

  /**
   * The word occurrences.
   */
  public long count;

  /**
   * Empty constructor.
   */
  public WordWithCount() { }

  /**
   * Creates a new tuple.
   * @param word the word.
   * @param count the word occurrences.
   */
  public WordWithCount(String word, long count) {
    this.word = word;
    this.count = count;
  }

  @Override
  public String toString() {
    return word + " : " + count;
  }

  public static WordWithCount valueOf(String s) {
    String fields[] = s.split(",", -1);

    if (fields.length != 2) {
      throw new IllegalArgumentException("Malformed WordWithCount");
    }

    String word = fields[0];
    long count = Long.valueOf(fields[1]);

    return new WordWithCount(word, count);
  }
}
