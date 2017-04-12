package gaiasim.agent;

import java.io.ObjectInputStream;
import java.io.InputStream;
import java.net.Socket;

import gaiasim.comm.ControlMessage;

public class Receiver implements Runnable {
    public Socket sd_;
    public InputStream in_;
    public ObjectInputStream oin_;

    public Receiver(Socket client_sd) throws java.io.IOException {
        sd_ = client_sd;
        in_ = client_sd.getInputStream();
        oin_ = new ObjectInputStream(client_sd.getInputStream());
    }

    public void run() {
        byte[] buffer = new byte[5];
        int num_recv;
        while (true) {
            try {
                ControlMessage c = (ControlMessage) oin_.readObject();
                System.out.println(c.type_);
                /*num_recv = in_.read(buffer);
                if (num_recv < 0) {
                    break;
                }
                System.out.println("Received " + num_recv);
                System.out.println(buffer);*/
            }
            catch (java.io.IOException e) {
                break;
            }
            catch (java.lang.ClassNotFoundException e) {
                e.printStackTrace();
                break;
            }
        }
        
        try {
            sd_.close();
        }
        catch (java.io.IOException e) {
            System.out.println("Error closing socket");
            e.printStackTrace();
            System.exit(1);
        }
    }
}

