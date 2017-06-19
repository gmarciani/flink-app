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
package com.gmarciani.flink_scaffolding.query3.keyer;

import com.gmarciani.flink_scaffolding.common.tuple.WordWithCount;
import org.apache.flink.api.java.functions.KeySelector;

/**
 * A key selector for {@link com.gmarciani.flink_scaffolding.common.tuple.WordWithCount}.
 * Used in {@link com.gmarciani.flink_scaffolding.query3.Query3}.
 *
 * @author Giacomo Marciani {@literal <gmarciani@acm.org>}
 * @since 1.0
 */
public class WordKeyer implements KeySelector<WordWithCount, String> {
  /**
   * User-defined function that extracts the key from an arbitrary object.
   * <p>
   * For example for a class:
   * <pre>
   * 	public class Word {
   * 		String word;
   * 		int count;
   *    }
   * </pre>
   * The key extractor could return the word as
   * a key to group all Word objects by the String they contain.
   * <p>
   * The code would look like this
   * <pre>
   * 	public String getKey(Word w) {
   * 		return w.word;
   *  }
   * </pre>
   *
   * @param value The object to get the key from.
   * @return The extracted key.
   * @throws Exception Throwing an exception will cause the execution of the respective task to fail,
   *                   and trigger recovery or cancellation of the program.
   */
  @Override
  public String getKey(WordWithCount value) throws Exception {
    return value.word;
  }
}
