package gaiasim.agent;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.InputStream;
import java.net.Socket;

public class Receiver implements Runnable {
    private static final Logger logger = LogManager.getLogger();
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
                    logger.info("SocketInputStream.read() returns {}" , num_recv);
                    break;
                }
            }
            catch (java.io.IOException e) {
                logger.error("IOException caught");
                break;
            }
        }

        logger.info("Closing socket from {}" , sd_.getRemoteSocketAddress());
        
        try {
            sd_.close();
        }
        catch (java.io.IOException e) {
            logger.error("Error closing socket");
            e.printStackTrace();
//            System.exit(1);
        }
    }
}

