package com.aspire.sc.mqtt;

import org.jetlinks.core.Value;
import org.jetlinks.core.defaults.Authenticator;
import org.jetlinks.core.device.*;
import org.springframework.util.StringUtils;
import reactor.core.publisher.Mono;

import javax.annotation.Nonnull;

public class JetLinksAuthenticator implements Authenticator {

    @Override
    public Mono<AuthenticationResponse> authenticate(@Nonnull AuthenticationRequest request, @Nonnull DeviceRegistry registry) {
        MqttAuthenticationRequest mqtt = ((MqttAuthenticationRequest) request);

        return registry
                .getDevice(mqtt.getClientId())
                .flatMap(device -> authenticate(request, device));
    }

    @Override
    public Mono<AuthenticationResponse> authenticate(@Nonnull AuthenticationRequest request, @Nonnull DeviceOperator deviceOperation) {
        if (request instanceof MqttAuthenticationRequest) {
            MqttAuthenticationRequest mqtt = ((MqttAuthenticationRequest) request);
            String username = mqtt.getUsername();
            String password = mqtt.getPassword();
            try {
                return deviceOperation.getConfigs("secureId", "secureKey")
                        .map(conf -> {
                            String secureId =  conf.getValue("secureId").map(Value::asString).orElse(null);
                            String secureKey = conf.getValue("secureKey").map(Value::asString).orElse(null);
                            if (!StringUtils.hasText(secureId) && !StringUtils.hasText(secureKey)) {
                                return AuthenticationResponse.success(deviceOperation.getDeviceId());
                            }
                            if (username.equals(secureId) && password.equals(secureKey)) {
                                return AuthenticationResponse.success(deviceOperation.getDeviceId());
                            } else {
                                return AuthenticationResponse.error(401, "密钥错误");
                            }
                        });
            } catch (NumberFormatException e) {
                return Mono.just(AuthenticationResponse.error(401, "用户名格式错误"));
            }
        }
        return Mono.just(AuthenticationResponse.error(400, "不支持的授权类型:" + request));
    }
}
