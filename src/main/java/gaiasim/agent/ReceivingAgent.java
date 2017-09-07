package gaiasim.agent;

import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;

import gaiasim.agent.Receiver;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class ReceivingAgent {

    private static final Logger logger = LogManager.getLogger();

    public static void main(String[] args) {
        int port = 33330;
        ServerSocket sd;

        int conn_cnt = 0;

        if(args.length == 1){
            port = Integer.parseInt(args[0]);
            logger.info("Running with 1 argument, set port to {}" , port);
        }

        try {
            sd = new ServerSocket(port);
            sd.setReceiveBufferSize(64*1024*1024);
            System.err.println("DEBUG, serversocket buffer: " + sd.getReceiveBufferSize());
//            sd.setSoTimeout(0);

            Runtime.getRuntime().addShutdownHook(new Thread(){public void run(){
                try {
                    sd.close();
                    System.out.println("The server is shut down!");
                } catch (IOException e) { System.exit(1); }
            }});

            while (true) {
                Socket dataSocket = sd.accept();
                conn_cnt ++;
//                dataSocket.setSendBufferSize(16*1024*1024);
                dataSocket.setReceiveBufferSize(64*1024*1024);
                System.err.println("dataSoc buf " + dataSocket.getReceiveBufferSize());
//                dataSocket.setSoTimeout(0);
//                dataSocket.setKeepAlive(true);
                logger.info( "{} Got a connection from {}", conn_cnt , dataSocket.getRemoteSocketAddress().toString());
                (new Thread(new Receiver(dataSocket))).start();
            }
        }
        catch (java.io.IOException e) {
            e.printStackTrace();
//            System.exit(1);
        }
    }
}
