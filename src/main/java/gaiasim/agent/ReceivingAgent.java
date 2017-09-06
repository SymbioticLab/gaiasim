package gaiasim.agent;

import java.net.ServerSocket;
import java.net.Socket;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import gaiasim.agent.Receiver;

public class ReceivingAgent {

    // also use Thread Pool on the receiver side
    static LinkedBlockingQueue<Runnable> taskQueue = new LinkedBlockingQueue<>();
    static ExecutorService es = new ThreadPoolExecutor(10,500,5000, TimeUnit.MILLISECONDS , taskQueue);

    public static void main(String[] args) {
        int port = 33330;
        ServerSocket sd;

        try {
            sd = new ServerSocket(port);
            while (true) {
                Socket dataSoc = sd.accept();
                dataSoc.setSendBufferSize(64*1024*1024);
                dataSoc.setReceiveBufferSize(64*1024*1024);
                System.out.println("Got a connection");

//                (new Thread(new Receiver(client))).start();
//                taskQueue.put(new Receiver(client));
                es.submit(new Receiver(dataSoc));
            }
        }
        catch (java.io.IOException e) {
            e.printStackTrace();
            System.exit(1);
        }
    }
}
