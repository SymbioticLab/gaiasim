package gaiasim.agent;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.InputStream;
import java.net.Socket;

public class Receiver implements Runnable {

    private static final Logger logger = LogManager.getLogger();

    public Socket sd_;
    public InputStream in_;

    long recvBytes = 0;

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
                    logger.info("Received -1 with socket from {}", sd_.getRemoteSocketAddress());
                    break;
                } else {
                    recvBytes += num_recv;
                }
            }
            catch (java.io.IOException e) {
                logger.error("Exception caught with socket from {}", sd_.getRemoteSocketAddress());
                e.printStackTrace();
                break;
            }
        }
        
        try {
            in_.close();
            sd_.close();
            logger.info("Closed socket from {}, received {} Bytes", sd_.getRemoteSocketAddress() , recvBytes);
        }
        catch (java.io.IOException e) {
            logger.error("Error closing socket");
            e.printStackTrace();
            System.exit(1);
        }
    }
}

