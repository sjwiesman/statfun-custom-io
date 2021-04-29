package com.ververica.statfun.io;

import com.google.protobuf.Message;
import com.ververica.statfun.io.elastic.TypedValueSinkFunction;
import com.ververica.statfun.io.rabbit.AutoRoutableDeserializer;
import org.apache.flink.statefun.flink.core.protorouter.AutoRoutableProtobufRouter;
import org.apache.flink.statefun.flink.io.datastream.SinkFunctionSpec;
import org.apache.flink.statefun.flink.io.datastream.SourceFunctionSpec;
import org.apache.flink.statefun.flink.io.generated.RoutingConfig;
import org.apache.flink.statefun.flink.io.generated.TargetFunctionType;
import org.apache.flink.statefun.sdk.io.EgressIdentifier;
import org.apache.flink.statefun.sdk.io.IngressIdentifier;
import org.apache.flink.statefun.sdk.reqreply.generated.TypedValue;
import org.apache.flink.statefun.sdk.spi.StatefulFunctionModule;
import org.apache.flink.streaming.connectors.elasticsearch7.ElasticsearchSink;
import org.apache.flink.streaming.connectors.rabbitmq.RMQSource;
import org.apache.flink.streaming.connectors.rabbitmq.common.RMQConnectionConfig;
import org.apache.http.HttpHost;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;

public class Module implements StatefulFunctionModule {

    private static final IngressIdentifier<Message> RABBIT_ID =
            new IngressIdentifier<>(Message.class, "com.ververica.ingress", "rmq");

    private static final EgressIdentifier<TypedValue> ELASTIC_ID =
            new EgressIdentifier<>("com.ververica.egress", "elastic", TypedValue.class);

    public void configure(Map<String, String> map, Binder binder) {
        rabbitMQ(binder);
        elastic(binder);
    }

    public void rabbitMQ(Binder binder) {
        RoutingConfig routingConfig = RoutingConfig.newBuilder()
                .setTypeUrl("com.ververica.types/report")
                .addAllTargetFunctionTypes(Collections.singletonList(
                        TargetFunctionType.newBuilder()
                                .setNamespace("com.ververica.fn")
                                .setType("server")
                                .build()))
                .build();

        RMQConnectionConfig rmqConfig = new RMQConnectionConfig.Builder()
                .setHost("localhost")
                .setPort(5000)
                .build();

        RMQSource<Message> source = new RMQSource<>(
                rmqConfig,
                "iot-metrics",
                new AutoRoutableDeserializer(routingConfig));

        SourceFunctionSpec<Message> spec = new SourceFunctionSpec<>(RABBIT_ID, source);
        binder.bindIngress(spec);
        binder.bindIngressRouter(RABBIT_ID, new AutoRoutableProtobufRouter());
    }

    public void elastic(Binder binder) {
        List<HttpHost> hosts = Arrays.asList(
                new HttpHost("127.0.0.1", 9200, "http"),
                new HttpHost("10.2.3.1", 9200, "http"));

        ElasticsearchSink.Builder<TypedValue> builder =
                new ElasticsearchSink.Builder<>(hosts, new TypedValueSinkFunction());

        SinkFunctionSpec<TypedValue> spec = new SinkFunctionSpec<>(ELASTIC_ID, builder.build());
        binder.bindEgress(spec);
    }
}
