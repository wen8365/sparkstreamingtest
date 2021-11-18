package wd.demo.wbskafka.controller;

import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.ResponseBody;

@Controller
public class WebSocketController {

    @RequestMapping("/testwebsocket")
    public String testWebSoket() {
        return "websockettest";
    }
}
