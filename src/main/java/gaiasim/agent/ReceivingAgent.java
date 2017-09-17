package gaiasim.agent;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.concurrent.Executors;

import gaiasim.util.Constants;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import org.jboss.netty.bootstrap.ServerBootstrap;
import org.jboss.netty.channel.ChannelPipeline;
import org.jboss.netty.channel.ChannelPipelineFactory;
import org.jboss.netty.channel.Channels;
import org.jboss.netty.channel.socket.nio.NioServerSocketChannelFactory;

public class ReceivingAgent {

    private static final Logger logger = LogManager.getLogger();

    public static void main(String[] args) {

        int port = 33330;

        if(args.length == 1){
            port = Integer.parseInt(args[0]);
            logger.info("Running with 1 argument, set port to {}" , port);
        }

        startNormalServer(port);
//        startNettyServer(port);

    }

    private static void startNettyServer(int port) {
        // start the discard server
        // Configure the server.
        ServerBootstrap bootstrap = new ServerBootstrap(
                new NioServerSocketChannelFactory(
                        Executors.newCachedThreadPool(),
                        Executors.newCachedThreadPool()));

        // Set up the pipeline factory.
        bootstrap.setPipelineFactory(new ChannelPipelineFactory() {
            public ChannelPipeline getPipeline() throws Exception {
                return Channels.pipeline(new DiscardServerHandler());
            }
        });
        bootstrap.setOption("child.tcpNoDelay", true);
        bootstrap.setOption("child.keepAlive", true);

        // TODO buffer size
/*        bootstrap.setOption("child.sendBufferSize", 1048576);
        bootstrap.setOption("child.receiveBufferSize", 1048576);*/

        // Bind and start to accept incoming connections.
        bootstrap.bind(new InetSocketAddress(port));
    }

    private static void startNormalServer(int port) {

        ServerSocket sd;
        int conn_cnt = 0;

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
                dataSocket.setSoTimeout(Constants.DEFAULT_SOCKET_TIMEOUT);
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
