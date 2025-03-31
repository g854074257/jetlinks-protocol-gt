package com.aspire.sc.mqtt;

import org.jetlinks.core.ProtocolSupport;
import org.jetlinks.core.defaults.CompositeProtocolSupport;
import org.jetlinks.core.message.codec.DefaultTransport;
import org.jetlinks.core.route.Route;
import org.jetlinks.core.spi.ProtocolSupportProvider;
import org.jetlinks.core.spi.ServiceContext;
import org.jetlinks.supports.official.JetLinksDeviceMetadataCodec;
import reactor.core.publisher.Mono;

import java.util.Arrays;
import java.util.Objects;
import java.util.stream.Collectors;

/**
 * 将自定义编解码器注册到协议
 *
 * @author guitao
 * @since 2024.6.17
 */
public class MqttProtocolSupportProvider implements ProtocolSupportProvider {
    @Override
    public Mono<? extends ProtocolSupport> create(ServiceContext serviceContext) {
        CompositeProtocolSupport support = new CompositeProtocolSupport();
        // 协议ID
        support.setId("iot-capex");
        // 协议名称
        support.setName("iot-capex");
        // 协议说明
        support.setDescription("iot-capex自定义MQTT协议");
        // 物模型编解码，固定为JetLinksDeviceMetadataCodec
        support.setMetadataCodec(new JetLinksDeviceMetadataCodec());
        // 协议认证器
        support.addAuthenticator(DefaultTransport.MQTT, new JetLinksAuthenticator());
        support.addRoutes(DefaultTransport.MQTT, Arrays
            .stream(TopicMessageCodec.values())
            .map(TopicMessageCodec::getRoute)
            .filter(Objects::nonNull)
            .collect(Collectors.toList())
        );
        // MQTT消息编解码器
        MqttDeviceMessageCodec codec = new MqttDeviceMessageCodec();
        // 两个参数，协议支持和编解码类MqttDeviceMessageCodec中保持一致，第二个参数定义使用的编码解码类
        support.addMessageCodecSupport(DefaultTransport.MQTT, () -> Mono.just(codec));
        return Mono.just(support);
    }
}
