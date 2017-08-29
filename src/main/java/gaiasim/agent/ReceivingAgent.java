package gaiasim.agent;

import java.net.ServerSocket;
import java.net.Socket;

import gaiasim.agent.Receiver;

public class ReceivingAgent {

    public static void main(String[] args) {
        int port = 33330;
        ServerSocket sd;

        int conn_cnt = 0;

        if(args.length == 1){
            port = Integer.parseInt(args[0]);
            System.out.println("Running with 1 argument, set port to " + port);
        }

        try {
            sd = new ServerSocket(port);
            sd.setSoTimeout(0);
            while (true) {
                Socket client = sd.accept();
                conn_cnt ++;
                client.setSoTimeout(0);
                client.setKeepAlive(true);
                System.out.println( conn_cnt + " Got a connection from " + client.getRemoteSocketAddress().toString());
                (new Thread(new Receiver(client))).start();
            }
        }
        catch (java.io.IOException e) {
            e.printStackTrace();
//            System.exit(1);
        }
    }
}
