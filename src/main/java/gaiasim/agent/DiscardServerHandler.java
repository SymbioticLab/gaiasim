package gaiasim.agent;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jboss.netty.channel.ChannelHandler;
import org.jboss.netty.channel.ChannelPipeline;
import org.jboss.netty.channel.SimpleChannelUpstreamHandler;

import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.buffer.ChannelBuffers;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelEvent;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.channel.ChannelStateEvent;
import org.jboss.netty.channel.ExceptionEvent;
import org.jboss.netty.channel.MessageEvent;
import org.jboss.netty.channel.SimpleChannelUpstreamHandler;

import org.jboss.netty.channel.ChannelFuture;
import org.jboss.netty.channel.ChannelFutureListener;

public class DiscardServerHandler extends SimpleChannelUpstreamHandler {
    private static final Logger logger = LogManager.getLogger();
    //    private final AtomicLong transferredBytes = new AtomicLong();
/*
    public long getTransferredBytes() {
        return transferredBytes.get();
    }
*/

    @Override
    public void handleUpstream(ChannelHandlerContext ctx, ChannelEvent e) throws Exception {
        if (e instanceof ChannelStateEvent) {
            logger.info(e.toString());
        }

        // Let SimpleChannelHandler call actual event handler methods below.
        super.handleUpstream(ctx, e);
    }
    @Override
    public void messageReceived(ChannelHandlerContext ctx, MessageEvent e) throws Exception {
        // Discard received data silently by doing nothing.
//        logger.info("MessageReceived");
//        transferredBytes.addAndGet(((ChannelBuffer) e.getMessage()).readableBytes());
//        Channel ch = e.getChannel();
//        ChannelBuffer buf = (ChannelBuffer) e.getMessage();
//	    System.out.println(buf);

/*        while (buf.readable()) {
            byte b[] = { buf.readByte() };
            System.out.println("sent");
            ch.write(ChannelBuffers.wrappedBuffer(b)) ;
        }
        System.out.println("doneWritten");*/

/*        ChannelFuture f = ch.write(buf);
        f.addListener(new ChannelFutureListener() {
            public void operationComplete(ChannelFuture future) {
                System.out.println("MessageSent");
                future.getChannel().close();
            }
        });*/
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, ExceptionEvent e) {
        // Close the connection when an exception is raised.
        logger.warn("Unexpected exception from downstream.\n{}",e.getCause());
        e.getChannel().close();
    }

    @Override
    public void channelConnected(ChannelHandlerContext ctx, ChannelStateEvent e) throws Exception {
        logger.info("Got a connection Client: {} . Server: {}" , e.getChannel().getRemoteAddress() , e.getChannel().getLocalAddress());
        super.channelConnected(ctx, e);
    }
}
