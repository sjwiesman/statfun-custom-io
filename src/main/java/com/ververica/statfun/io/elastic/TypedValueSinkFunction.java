package com.ververica.statfun.io.elastic;

import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.statefun.sdk.reqreply.generated.TypedValue;
import org.apache.flink.streaming.connectors.elasticsearch.ElasticsearchSinkFunction;
import org.apache.flink.streaming.connectors.elasticsearch.RequestIndexer;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.Requests;
import org.elasticsearch.common.xcontent.XContentType;

public class TypedValueSinkFunction implements ElasticsearchSinkFunction<TypedValue> {

    public void process(TypedValue value, RuntimeContext ctx, RequestIndexer indexer) {
        IndexRequest request = Requests.indexRequest()
                .index("status-report")
                .source(value.getValue().toByteArray(), XContentType.JSON);

        indexer.add(request);
    }
}
