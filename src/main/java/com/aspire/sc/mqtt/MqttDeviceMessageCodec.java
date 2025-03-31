package com.aspire.sc.mqtt;

import com.alibaba.fastjson.JSONObject;
import com.aspire.sc.mqtt.utils.BASE64DecodedMultipartFile;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.netty.buffer.Unpooled;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.codec.binary.Base64;
import org.apache.commons.io.FileUtils;
import org.jetlinks.core.device.DeviceConfigKey;
import org.jetlinks.core.message.*;
import org.jetlinks.core.message.codec.*;
import org.springframework.core.io.FileSystemResource;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.util.LinkedMultiValueMap;
import org.springframework.util.MultiValueMap;
import org.springframework.util.StringUtils;
import org.springframework.web.client.RestTemplate;
import org.springframework.web.multipart.MultipartFile;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import sun.misc.BASE64Decoder;

import javax.annotation.Nonnull;
import java.io.File;
import java.io.IOException;
import java.util.*;

/**
 * 自定义MQTT协议
 *
 * @author guitao
 * @since 2024.6.17
 */
@Slf4j
public class MqttDeviceMessageCodec implements DeviceMessageCodec {

    private final String IOT_CAPEX_BASE = "http://127.0.0.1:7700"; // 本地
    private final String IOT_CAPEX_XXJ = "/iot/getMessageFromXxj";
    private final String IOT_CAPEX_HKMJ = "/EalarmCallBack";

    private final Transport transport;

    private final ObjectMapper mapper;

    public MqttDeviceMessageCodec(Transport transport) {
        this.transport = transport;
        this.mapper = ObjectMappers.JSON_MAPPER;
    }

    public MqttDeviceMessageCodec() {
        this(DefaultTransport.MQTT);
    }

    /**
     * 传输协议定义
     *
     * @return result
     */
    @Override
    public Transport getSupportTransport() {
        return DefaultTransport.MQTT;
    }

    /**
     * 平台下发消息到设备
     *
     * @param context context
     * @return result
     */
    @Nonnull
    @Override
    public Mono<MqttMessage> encode(@Nonnull MessageEncodeContext context) {
        return Mono.defer(() -> {
            Message message = context.getMessage();

            if (message instanceof DisconnectDeviceMessage) {
                return ((ToDeviceMessageContext) context)
                    .disconnect()
                    .then(Mono.empty());
            }

            if (message instanceof DeviceMessage) {
                DeviceMessage deviceMessage = ((DeviceMessage) message);

                TopicPayload convertResult = TopicMessageCodec.encode(ObjectMappers.JSON_MAPPER, deviceMessage);
                if (convertResult == null) {
                    return Mono.empty();
                }
                return Mono
                    .justOrEmpty(deviceMessage.getHeader("productId").map(String::valueOf))
                    .switchIfEmpty(context.getDevice(deviceMessage.getDeviceId())
                                          .flatMap(device -> device.getSelfConfig(DeviceConfigKey.productId))
                    )
                    .defaultIfEmpty("null")
                    .map(productId -> SimpleMqttMessage
                        .builder()
                        .clientId(deviceMessage.getDeviceId())
                        .topic("/".concat(productId).concat(convertResult.getTopic()))
                        .payloadType(MessagePayloadType.JSON)
                        .payload(Unpooled.wrappedBuffer(convertResult.getPayload()))
                        .build());
            } else {
                return Mono.empty();
            }
        });
    }

    /**
     * 设备上报发送数据
     *
     * @param context context
     * @return result
     */
    @Nonnull
    @Override
    public Flux<DeviceMessage> decode(@Nonnull MessageDecodeContext context) {
        MqttMessage message = (MqttMessage) context.getMessage();
        byte[] payload = message.payloadAsBytes();
        // TODO 自定义处理设备上报逻辑(信息机，闸机)
        // 从上下文中获取消息字节数组
        String text=new String(payload);
        dealMessageFromEquipment(text, context);
        return TopicMessageCodec
            .decode(mapper, TopicMessageCodec.removeProductPath(message.getTopic()), payload)
            //如果不能直接解码，可能是其他设备功能
            .switchIfEmpty(FunctionalTopicHandlers
                               .handle(context.getDevice(),
                                       message.getTopic().split("/"),
                                       payload,
                                       mapper,
                                       reply -> doReply(context, reply)))
            ;
    }

    private Mono<Void> doReply(MessageCodecContext context, TopicPayload reply) {

        if (context instanceof FromDeviceMessageContext) {
            return ((FromDeviceMessageContext) context)
                .getSession()
                .send(SimpleMqttMessage
                          .builder()
                          .topic(reply.getTopic())
                          .payload(reply.getPayload())
                          .build())
                .then();
        } else if (context instanceof ToDeviceMessageContext) {
            return ((ToDeviceMessageContext) context)
                .sendToDevice(SimpleMqttMessage
                                  .builder()
                                  .topic(reply.getTopic())
                                  .payload(reply.getPayload())
                                  .build())
                .then();
        }
        return Mono.empty();
    }


    private void dealMessageFromEquipment(String text, MessageDecodeContext context) {
        try {
            if (!StringUtils.hasLength(text)) {
                return;
            }
            JSONObject jsonObject = JSONObject.parseObject(text);
            JSONObject prop = (JSONObject) jsonObject.get("properties");
            if (prop == null) {
                return;
            }
            Object equipment = prop.get("equipment");
            if (equipment == null) {
                return;
            }
            if ("xxj".equals(equipment.toString())) {
                dealMessageFromXxj(text, context);
            } else if ("hkmj".equals(equipment.toString())) {
                dealMessageFromHkmj(text, context);
            }
        } catch (Exception e) {
            log.error("处理设备消息异常：" + text, e);
        }
    }

    /**
     * 处理闸机上报消息处理逻辑方法
     *
     * @param text text
     * @param context context
     */
    private void dealMessageFromHkmj(String text, MessageDecodeContext context) {
        MqttMessage message = (MqttMessage) context.getMessage();
        // 获取topic的信息,并改造成自定义topic返回
        String[] split = message.getTopic().split("/");
        StringBuilder topic = new StringBuilder();
        for (int i = 1; i < 3; i++) {
            topic.append("/").append(split[i]);
        }
        topic.append("/properties/callback/hkmj");
        String result = getMessageFromHkmjWeb(text);
        SimpleMqttMessage simpleMqttMessage = SimpleMqttMessage.builder()
            .topic(topic.toString())
            .payload(result)
            .build();
        Mono<Boolean> send = ((FromDeviceMessageContext) context)
            .getSession()
            .send(simpleMqttMessage);
        send.subscribe(value -> {
            log.info("处理设备消息并返回下发成功:" + simpleMqttMessage);
        });
    }

    private String getMessageFromHkmjWeb(String text) {
        String result = "";
        Set<String> tempFilePath = new HashSet<>();
        try {
            JSONObject jsonObject = JSONObject.parseObject(text);
            JSONObject properties = (JSONObject) jsonObject.get("properties");
            String msg = Objects.isNull(properties.get("message")) ? "" : properties.get("message").toString();
            String spKey =  Objects.isNull(properties.get("spKey")) ? "" : properties.get("spKey").toString();
            String spKeySecret =  Objects.isNull(properties.get("spKeySecret")) ? "" : properties.get("spKeySecret").toString();
            String timestamp = Objects.isNull(properties.get("timestamp")) ? "" :  properties.get("timestamp").toString();
            RestTemplate rest = new RestTemplate();
            // 设置表单数据
            MultiValueMap<String, Object> formData = new LinkedMultiValueMap<>();
            formData.add("data", msg);
            // 将base6格式解析为图片
            getfileByBase64(properties, formData, tempFilePath);
            // 设置请求头信息
            HttpHeaders headers = new HttpHeaders();
            headers.set("Content-Type", "multipart/form-data");
            headers.set("spKey", spKey);
            headers.set("spKeySecret", spKeySecret);
            headers.set("timestamp", timestamp);
            // 创建HttpEntity，包装表单数据和头信息
            HttpEntity<MultiValueMap<String, Object>> requestEntity = new HttpEntity<>(formData, headers);
            result = rest.postForObject(IOT_CAPEX_BASE + IOT_CAPEX_HKMJ, requestEntity, String.class);
        } catch (Exception e) {
            log.error("处理设备消息并返回下发异常", e);
            JSONObject jsonObject = new JSONObject();
            jsonObject.put("code", "500");
            jsonObject.put("message", "调用道闸设备设备服务异常");
            jsonObject.put("timestamp", System.currentTimeMillis());
            result = jsonObject.toJSONString();
        } finally {
            // 删除临时的文件
            if(!tempFilePath.isEmpty()){
                for(String path : tempFilePath){
                    File file = new File(path);
                    if(file.exists()){
                        file.delete();
                    }
                }
            }
        }
        return result;
    }

    private void getfileByBase64(JSONObject properties, MultiValueMap<String, Object> formData, Set<String> tempFilePath) {
        try {
            if (Objects.isNull(properties.get("file"))) {
                log.info("file图片为空");
            }
            String file = properties.get("file").toString();
            if (!StringUtils.hasLength(file)) {
                log.info("file图片为空");
                return;
            }
            // 将base64转换为MultipartFile格式
            MultipartFile multipartFile = base64ToMultipartFile(file);
            //生成临时的文件,完成传输后再删除
            byte[] bytes = Base64.decodeBase64(file);
            File tempFile = new File(UUID.randomUUID() + multipartFile.getOriginalFilename());
            tempFilePath.add(tempFile.getAbsolutePath());
            //file.transferTo(tempFile);
            FileUtils.copyInputStreamToFile(multipartFile.getInputStream(), tempFile);
            FileSystemResource fileSystemResource = new FileSystemResource(tempFile);
            formData.add("file", fileSystemResource);
            log.info("file图片解析成功：" + tempFile.getAbsolutePath());
        } catch (Exception e) {
            log.error("解析图片错误", e);
        }
    }

    public static MultipartFile base64ToMultipartFile(String s) {
        MultipartFile image = null;
        StringBuilder base64 = new StringBuilder("");
        if (s != null && !"".equals(s)) {
            base64.append(s);
            String[] baseStrs = base64.toString().split(",");
            BASE64Decoder decoder = new BASE64Decoder();
            byte[] b = new byte[0];
            try {
                b = decoder.decodeBuffer(baseStrs[1]);
            } catch (IOException e) {
                log.error("转换图片失败" + s, e);
            }
            for (int j = 0; j < b.length; ++j) {
                if (b[j] < 0) {
                    b[j] += 256;
                }
            }
            image = new BASE64DecodedMultipartFile(b, baseStrs[0]);
        }
        return image;
    }

    /**
     * 处理信息机上报消息处理逻辑方法
     *
     * @param text text
     * @param ctx ctx
     */
    private void dealMessageFromXxj(String text, MessageDecodeContext ctx) {
        MqttMessage mqttMessage = (MqttMessage) ctx.getMessage();
        // 获取topic的信息,并改造成自定义topic返回
        String[] split = mqttMessage.getTopic().split("/");
        StringBuilder topic = new StringBuilder();
        for (int i = 1; i < 3; i++) {
            topic.append("/").append(split[i]);
        }
        topic.append("/properties/callback/xxj");
        String result = getMessageFromXxjWeb(text);
        SimpleMqttMessage simpleMqttMessage = SimpleMqttMessage.builder()
            .topic(topic.toString())
            .payload(result)
            .build();
        Mono<Boolean> send = ((FromDeviceMessageContext) ctx)
            .getSession()
            .send(simpleMqttMessage);
        send.subscribe(value -> {
            log.info("处理设备消息并返回下发成功:" + simpleMqttMessage);
        });
    }

    private String getMessageFromXxjWeb(String text) {
        String result = "";
        try {
            // 处理设备上报的text信息，获取实际数据,ip,端口,以及验证信息
            JSONObject jsonObject = JSONObject.parseObject(text);
            JSONObject properties = (JSONObject) jsonObject.get("properties");
            RestTemplate rest = new RestTemplate();
            result = rest.postForObject(IOT_CAPEX_BASE + IOT_CAPEX_XXJ, properties, String.class);
        } catch (Exception e) {
            log.error("处理设备消息并返回下发异常", e);
            JSONObject json = new JSONObject();
            json.put("code", "500");
            json.put("message", "调用信息机设备服务异常");
            json.put("timestamp", System.currentTimeMillis());
            result = json.toJSONString();
        }
        return result;
    }

    //    /**
//     * 设备上报发送数据
//     *
//     * @param context context
//     * @return result
//     */
//    @Nonnull
//    @Override
//    public Flux<DeviceMessage> decode(@Nonnull MessageDecodeContext context) {
//        return Flux.defer(() -> {
//            // 消息上下文
//            FromDeviceMessageContext ctx = ((FromDeviceMessageContext) context);
//            // 从上下文中获取消息字节数组
//            ByteBuf byteBuf = context.getMessage().getPayload();
//            byte[] payload = ByteBufUtil.getBytes(byteBuf, 0, byteBuf.readableBytes(), false);
//            // 把字节流转换为字符串，根据不同设备不同协议进行解析，
//            String text=new String(payload);
//            ReportPropertyMessage message = new ReportPropertyMessage();
//            // 设置消息ID为我们获得的消息内容
//            message.setDeviceId(text);
//            // 以当前时间戳为消息时间
//            long time= LocalDateTime.now().toInstant(ZoneOffset.of("+8")).toEpochMilli();
//            message.setTimestamp(time);
//            // 构造上报属性
//            Map<String, Object> properties = new HashMap<>();
//            properties.put("text",text);
//            // 设置上报属性
//            message.setProperties(properties);
//
//            // TODO 自定义处理设备上报逻辑(信息机，闸机)
//            dealMessageFromEquipment(text, context);
//
//            // 获取设备会话信息
//            DeviceSession session = ctx.getSession();
//            // 如果session中没有设备信息，则为设备首次上线
//            if (session.getOperator() == null) {
//                DeviceOnlineMessage onlineMessage = new DeviceOnlineMessage();
//                onlineMessage.setDeviceId(text);
//                onlineMessage.setTimestamp(System.currentTimeMillis());
//                // 返回到平台上线消息
//                return Flux.just(message,onlineMessage);
//            }
//            // 返回到平台属性上报消息
//            return Mono.just(message);
//        });
//    }

}
