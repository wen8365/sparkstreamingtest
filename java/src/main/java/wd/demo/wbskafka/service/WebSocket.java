package wd.demo.wbskafka.service;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import javax.websocket.*;
import javax.websocket.server.ServerEndpoint;
import java.util.concurrent.CopyOnWriteArraySet;

@ServerEndpoint("/webSocket")
@Component
public class WebSocket {

    private Session session;
    public static CopyOnWriteArraySet<WebSocket> webSocketSet = new CopyOnWriteArraySet<WebSocket>();

    private static final Logger LOGGER = LoggerFactory.getLogger(WebSocket.class);

    @OnOpen
    public void onOpen(Session session) throws Exception {
        this.session = session;
        webSocketSet.add(this);
    }

    @OnClose
    public void onClose() {
        webSocketSet.remove(this);
        LOGGER.info("Websocket close");
    }

    @OnMessage
    public void onMessage(String message, Session session) {
        LOGGER.info("Receive message from client: " + message);
    }

    @OnError
    public void onError(Session session, Throwable error) {
        LOGGER.error("Error", error);
    }

    public void sendMessage(String message) throws Exception {
        if (this.session.isOpen()) {
            this.session.getBasicRemote().sendText(message);
        }
    }
}
