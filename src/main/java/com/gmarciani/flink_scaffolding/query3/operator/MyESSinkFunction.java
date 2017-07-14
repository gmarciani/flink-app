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

import com.gmarciani.flink_scaffolding.query2.tuple.WindowWordRanking;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.streaming.connectors.elasticsearch.ElasticsearchSinkFunction;
import org.apache.flink.streaming.connectors.elasticsearch.RequestIndexer;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.Requests;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

/**
 * A sink that writes {@link WindowWordRanking} to Elasticsearch.
 *
 * @author Giacomo Marciani {@literal <gmarciani@acm.org>}
 * @since 1.0
 */
public class MyESSinkFunction implements ElasticsearchSinkFunction<WindowWordRanking> {

  /**
   * The logger.
   */
  private static final Logger LOG = LoggerFactory.getLogger(MyESSinkFunction.class);

  /**
   * The name of Elasticsearch index.
   */
  private String indexName;

  /**
   * The name of Elasticsearch type.
   */
  private String typeName;

  /**
   * Creates a new {@link MyESSinkFunction} with the specified index and type.
   * @param indexName the name of Elasticsearch index.
   * @param typeName the name of Elasticsearch type.
   */
  public MyESSinkFunction(String indexName, String typeName) {
    this.indexName = indexName;
    this.typeName = typeName;
  }

  @Override
  public void process(WindowWordRanking value, RuntimeContext ctx, RequestIndexer indexer) {
    indexer.add(this.createWindowWordRanking(value));
  }

  /**
   * Creates a new Elasticsearch request from the given element.
   * @param value the element to process.
   * @return the Elasticsearch request.
   */
  public IndexRequest createWindowWordRanking(WindowWordRanking value) {
    Map<String,String> json = new HashMap<>();
    json.put("wStart", String.valueOf(value.getWStart()));
    json.put("wEnd", String.valueOf(value.getWStop()));
    json.put("rank", String.valueOf(value.getRank()));

    LOG.debug("JSON: {}", json);

    return Requests.indexRequest()
        .index(this.indexName)
        .type(this.typeName)
        .source(json);
  }
}
