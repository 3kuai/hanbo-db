package com.lmx.jredis.transport.jsocket;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.annotation.Order;
import org.springframework.stereotype.Service;

import javax.annotation.PostConstruct;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.ServerSocket;

/**
 * Created by Administrator on 2017/12/16.
 */
@Service
@Order(3)
public class SocketServer {
    @Autowired
    RequestEventProducer requestEventProducer;

//    @PostConstruct
    public void init() {
        new Thread(new Runnable() {
            @Override
            public void run() {
                try {
                    startSocket();
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        }).start();
    }

    void startSocket() {
        try {
            ServerSocket serverSocket = new ServerSocket();
            serverSocket.bind(new InetSocketAddress("0.0.0.0", 16381));
            System.err.printf("jRedis bio server listening on port=%d \n", 16381);
            while (true) {
                requestEventProducer.onData(serverSocket.accept(), null);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}

