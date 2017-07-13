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

package com.gmarciani.flink_scaffolding;

import com.gmarciani.flink_scaffolding.common.ProgramDriver;
import com.gmarciani.flink_scaffolding.query1.TopologyQuery1;
import com.gmarciani.flink_scaffolding.query2.TopologyQuery2;
import com.gmarciani.flink_scaffolding.query3.TopologyQuery3;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The app entry-point as programs driver.
 * @author Giacomo Marciani {@literal <gmarciani@acm.org>}
 * @since 1.0
 */
public class Driver {

  /**
   * The logger.
   */
  private static final Logger LOG = LoggerFactory.getLogger(Driver.class);


  /**
   * Thedriver main method.
   * @param args the command line arguments.
   */
  public static void main(String[] args) throws Exception {
    int exitCode = -1;
    ProgramDriver driver = new ProgramDriver();

    try {

    /* *********************************************************************************************
     * QUERY 1
     **********************************************************************************************/
      driver.addClass(TopologyQuery1.PROGRAM_NAME, TopologyQuery1.class, TopologyQuery1.PROGRAM_DESCRIPTION);

    /* *******************************************************************************************
     * QUERY 2
     **********************************************************************************************/
      driver.addClass(TopologyQuery2.PROGRAM_NAME, TopologyQuery2.class, TopologyQuery2.PROGRAM_DESCRIPTION);

    /* *******************************************************************************************
     * QUERY 3
     **********************************************************************************************/
      driver.addClass(TopologyQuery3.PROGRAM_NAME, TopologyQuery3.class, TopologyQuery3.PROGRAM_DESCRIPTION);

    /* *******************************************************************************************
     * QUERY 4
     **********************************************************************************************/
      driver.addClass(TopologyQuery4.PROGRAM_NAME, TopologyQuery4.class, TopologyQuery4.PROGRAM_DESCRIPTION);

      LOG.info("Running driver...");

      exitCode = driver.run(args);

    } catch (Throwable exc) {
      exc.printStackTrace();
      LOG.error(exc.getMessage());
    }

    System.exit(exitCode);
  }

}
