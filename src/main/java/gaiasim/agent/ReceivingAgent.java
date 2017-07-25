package gaiasim.agent;

import java.net.ServerSocket;
import java.net.Socket;

import gaiasim.agent.Receiver;

public class ReceivingAgent {

    public static void main(String[] args) {
        int port = 33330;
        ServerSocket sd;

        if(args.length == 1){
            port = Integer.parseInt(args[0]);
            System.out.println("Running with 1 argument, set port to " + port);
        }

        try {
            sd = new ServerSocket(port);
            while (true) {
                Socket client = sd.accept();
                System.out.println("Got a connection");
                (new Thread(new Receiver(client))).start();
            }
        }
        catch (java.io.IOException e) {
            e.printStackTrace();
//            System.exit(1);
        }
    }
}
