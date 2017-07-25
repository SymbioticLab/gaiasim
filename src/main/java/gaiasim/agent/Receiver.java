package gaiasim.agent;

import java.io.InputStream;
import java.net.Socket;

public class Receiver implements Runnable {
    public Socket sd_;
    public InputStream in_;

    public Receiver(Socket client_sd) throws java.io.IOException {
        sd_ = client_sd;
        in_ = client_sd.getInputStream();
    }

    public void run() {
        byte[] buffer = new byte[1024*1024];
        int num_recv;
        while (true) {
            try {
                num_recv = in_.read(buffer);
                if (num_recv < 0) {
                    System.err.println("SocketInputStream.read() returns " + num_recv);
//                    break;
                }
            }
            catch (java.io.IOException e) {
                System.err.println("IOException caught");
//                break;
            }
        }
        
//        try {
//            sd_.close();
//        }
//        catch (java.io.IOException e) {
//            System.out.println("Error closing socket");
//            e.printStackTrace();
////            System.exit(1);
//        }
    }
}

