package com.aspire.sc.mqtt;

import org.jetlinks.core.message.CommonDeviceMessage;
import org.jetlinks.core.message.MessageType;

public class CapexMessage extends CommonDeviceMessage<CapexMessage> {
    public CapexMessage() {
    }

    public MessageType getMessageType() {
        return MessageType.UNKNOWN;
    }
}
