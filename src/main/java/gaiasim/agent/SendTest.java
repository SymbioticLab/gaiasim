package gaiasim.agent;

import java.io.ObjectOutputStream;
import java.io.PrintWriter;
import java.net.Socket;

import gaiasim.comm.ControlMessage;

public class SendTest {
    public static void main(String[] args) {
        int server_port = 33330;

        try {
            Socket sd = new Socket("127.0.0.1", server_port);
            System.out.println("Connected on port " + sd.getLocalPort());
            /*PrintWriter out = new PrintWriter(sd.getOutputStream(), true);
            for (int i = 0; i < 10; i++) {
                Thread.sleep(1000);
                out.println("test");
            }*/
            ObjectOutputStream os = new ObjectOutputStream(sd.getOutputStream());
            for (int i = 0; i < 10; i++) {
                Thread.sleep(1000);
                os.writeObject(new ControlMessage(ControlMessage.Type.TERMINATE));
            }
            sd.close();
        }
        catch (java.io.IOException e) {
            e.printStackTrace();
            System.exit(1);
        }
        catch (InterruptedException e) {
            e.printStackTrace();
            System.exit(1);
        }
    }
}
