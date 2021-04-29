package com.ververica.statfun.io.rabbit;

import com.google.protobuf.Message;
import com.google.protobuf.MoreByteStrings;
import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Envelope;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.statefun.flink.io.generated.AutoRoutable;
import org.apache.flink.statefun.flink.io.generated.RoutingConfig;
import org.apache.flink.streaming.connectors.rabbitmq.RMQDeserializationSchema;

public class AutoRoutableDeserializer implements RMQDeserializationSchema<Message> {

    private final RoutingConfig config;

    public AutoRoutableDeserializer(RoutingConfig config) {
        this.config = config;
    }

    @Override
    public void deserialize(
            Envelope envelope,
            AMQP.BasicProperties properties,
            byte[] bytes,
            RMQCollector<Message> collector) {

        AutoRoutable routable = AutoRoutable.newBuilder()
                .setConfig(config)
                .setId(envelope.getRoutingKey())
                .setPayloadBytes(MoreByteStrings.wrap(bytes))
                .build();

        collector.collect(routable);
    }

    @Override
    public boolean isEndOfStream(Message autoRoutable) {
        return false;
    }

    @Override
    public TypeInformation<Message> getProducedType() {
        return TypeInformation.of(Message.class);
    }
}
