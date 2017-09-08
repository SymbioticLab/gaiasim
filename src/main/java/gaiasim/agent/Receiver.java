package gaiasim.agent;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.InputStream;
import java.net.Socket;
import java.util.concurrent.atomic.AtomicInteger;

public class Receiver implements Runnable {

    private static final Logger logger = LogManager.getLogger();

    private static AtomicInteger socketCnt = new AtomicInteger(0);

    public Socket sd_;
    public InputStream in_;

    long recvBytes = 0;

    public Receiver(Socket client_sd) throws java.io.IOException {
        sd_ = client_sd;
        in_ = client_sd.getInputStream();
        socketCnt.incrementAndGet();
        logger.info("Got connection from {} , buffer {} , total sockets {}", sd_.getRemoteSocketAddress() , sd_.getReceiveBufferSize(), socketCnt);
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
            logger.info("Closed socket from {}, received {} kB, remaining sockets: {}", sd_.getRemoteSocketAddress() , recvBytes/1024, socketCnt.decrementAndGet());
        }
        catch (java.io.IOException e) {
            logger.error("Error closing socket");
            e.printStackTrace();
            System.exit(1);
        }
    }
}

