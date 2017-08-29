package gaiasim.gaiaagent;

// A worker manages a persistent connection.

// The logic of Worker.run:
// if no subscription, then wait for subscription
// if total_rate > 0 , then
// first poll for subscription message, and process the message
// send data according to the rate (rateLimiter.acquire() )
// call distribute_transmitted to distribute the sent data to different subscribed FlowGroups.

// TODO in the future we may make the worker thread not bind to a particular connection
// so that we can use a thread pool to process on many connections.

import com.google.common.util.concurrent.RateLimiter;
import gaiasim.util.Constants;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.BufferedOutputStream;
import java.io.IOException;
import java.net.Socket;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

@SuppressWarnings("Duplicates")
public class WorkerThread implements Runnable{

    private static final Logger logger = LogManager.getLogger();
//    PConnection conn;
    AgentSharedData sharedData;

    String connID; // name of this TCP Connection. SA_id-RA_id.path_id
    String raID;
    int pathID;

    // Queue on which SendingAgent places updates for this PersistentConnection. Updates
    // flow, or that the PersistentConnection should terminate.
    // may inform the PersistentConnection of a new subscribing flow, an unsubscribing
    LinkedBlockingQueue<SubscriptionMessage> subcriptionQueue;


    // The subscription info should contain: FG_ID -> FGI and FG_ID -> rate
    // Note that the FGI.rate is not the rate here!
    public HashMap<String, SubscriptionInfo> subscribers = new HashMap<String, SubscriptionInfo>();

    // Current total rate requested by subscribers. This is the aggregate rate of the Data Center.
    public volatile double total_rate = 0.0;

    public Socket dataSocket;

    // data related

    private final RateLimiter rateLimiter;
    private byte[] data_block = new byte[Constants.BLOCK_SIZE_MB * 1024 * 1024]; // 32MB for now.

    private BufferedOutputStream bos;


    public WorkerThread(String workerID, String RAID, int pathID , Socket soc, LinkedBlockingQueue<SubscriptionMessage> inputQueue , AgentSharedData sharedData){
        this.connID = workerID;
        this.dataSocket = soc;
        this.subcriptionQueue = inputQueue;
        this.sharedData = sharedData;
        this.raID = RAID;
        this.pathID = pathID;

        rateLimiter = RateLimiter.create(Constants.DEFAULT_TOKEN_RATE);

        try {
            bos = new BufferedOutputStream(soc.getOutputStream() , Constants.BUFFER_SIZE );
        } catch (IOException e) {
            e.printStackTrace();
        }

        logger.info("WorkerThread {} created, src port {}", this.connID, this.dataSocket.getLocalPort());

    }

    @Override
    public void run() {
        SubscriptionMessage m = null;

        // await for the signal before starting sending Heartbeat Msgs
        try {
            sharedData.readySignal.await();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        rateLimiter.setRate(Constants.DEFAULT_TOKEN_RATE);

        int heartBeatCnt = 0;

        // use a single eventloop
        while (true){
            m = subcriptionQueue.poll();

            // process the msg
            processMessage(m);

            // do the job
            if (total_rate > 0){
                sendData(total_rate);
            }
            else {
                if (sharedData.isSendingHeartBeat.get()) {
                    heartBeatCnt++;
                    if (heartBeatCnt >= 100) {
                        sendHeartBeat();
                        heartBeatCnt = 0;
                    }
                }
            }

            // rate limiting
            rateLimiter.acquire(1);

            // what if the jobs are to heavy to finish in time?

        }
    }

    private void sendData(double total_rate) {
        try {
            // rate is MBit/s, converting to Block/s

            double cur_rate = total_rate;

            int data_length;

            // check if 100 permits/s is enough (3200MByte/s enough?)
            if( cur_rate < Constants.BLOCK_SIZE_MB * 8 * Constants.DEFAULT_TOKEN_RATE  ){
                // no need to change rate , calculate the length
                rateLimiter.setRate(Constants.DEFAULT_TOKEN_RATE);
                data_length = (int) (cur_rate / Constants.DEFAULT_TOKEN_RATE * 1024 * 1024 / 8);
            }
            else {
                data_length = Constants.BLOCK_SIZE_MB;
                double new_rate = cur_rate / 8 / Constants.BLOCK_SIZE_MB;
                logger.warn("Total rate {} too high for {}, setting new sending rate to {} / s", total_rate, this.connID, new_rate);
                rateLimiter.setRate(new_rate);
            }

            bos.write(data_block , 0, data_length);
            bos.flush();

//                    logger.info("Worker {} flushed {} Bytes at rate {} on {}", connID, data_length, total_rate, System.currentTimeMillis());
//                    logger.info("Worker {} flushed {} Bytes at rate {}", connID, data_length, total_rate);
//                    System.out.println("Worker: Flushed Writing " + data_length + " w/ rate: " + total_rate + " Mbit/s  @ " + System.currentTimeMillis());

            // distribute transmitted...
            double tx_ed = (double) data_length * 8 / 1024 / 1024;

            distribute_transmitted( tx_ed);
//                        System.out.println("T_MBit " + tx_ed + " original " + buffer_size_megabits_);
//                        data_.distribute_transmitted(buffer_size_megabits_);
        }
        catch (IOException e) {
//                    System.err.println("Fail to write data to ra");
            logger.error("Fail to write data to ra {} , thread {}", raID, connID);
            e.printStackTrace();
//                    System.exit(1); // don't fail here
        }
    }

    private void sendHeartBeat() {
        try {
            bos.write(1);
            bos.flush();
            logger.info("sending heartbeat from {}", this.connID);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private void processMessage(SubscriptionMessage m) {
        // m will be null only if poll() returned that we have no
        // messages. If m is not null, process the message.
        // use while loop to process all queued message.
        while (m != null) {

            // handles subscription message.

            // Now we only use the subscription message as a sync signal..

            if (m.getType() == SubscriptionMessage.MsgType.SYNC){
                // update the worker's subscription info
                // and go back to work.
                subscribers.clear();
                subscribers.putAll( sharedData.subscriptionRateMaps.get(raID).get(pathID) );

                total_rate = sharedData.subscriptionRateMaps.get(raID).get(pathID).values()
                        .stream().mapToDouble(SubscriptionInfo::getRate).sum();

                if (total_rate  > 0){
                    logger.debug("Worker {} Received SYNC message, now working with rate {} (MBit/s)", this.connID , total_rate);
                }

            }

/*                if (m.getType() == SubscriptionMessage.MsgType.SUBSCRIBE) {
                    if (m.getFgi().commit_subscription(data_.id_, m.ts_)) {
                        System.out.println("PersistentConn: Subscribing flow " + m.getFgi().id_ + " to " + data_.id_);
                        total_rate += m.getRate();
                        subscribers.put(m.flow_info_.id_, new Subscription(m.getFgi(), m.getRate()));
                    }
                }
                else if (m.getType()  == SubscriptionMessage.MsgType.UNSUBSCRIBE) {
                    if (m.getFgi().commit_unsubscription(data_.id_, m.ts_)) {
                        System.out.println("PersistentConn: Unsubscribing flow " + m.flow_info_.id_ + " from " + data_.id_);
                        s = subscribers.get(m.flow_info_.id_);
                        total_rate -= s.rate_;
                        data_.subscribers_.remove(m.flow_info_.id_);

                        // Ensure there aren't any rounding errors
                        if (subscribers.isEmpty()) {
                            total_rate = 0.0;
                        }
                    }
                }*/

//                else {
//                    // TERMINATE
////                    try {
////                        data_.dataSocket.close();
////                    }
////                    catch (java.io.IOException e) {
////                        e.printStackTrace();
////                        System.exit(1);
////                    }
////                    return;
//                }

            m = subcriptionQueue.poll();
        }
    }


    public synchronized void distribute_transmitted(double transmitted_MBit) {
        if (transmitted_MBit > 0.0 ) {

            ArrayList<SubscriptionInfo> to_remove = new ArrayList<SubscriptionInfo>();

            double flow_rate;
            for (Map.Entry<String, SubscriptionInfo> entry : subscribers.entrySet()) {
                SubscriptionInfo s = entry.getValue();
                FlowGroupInfo f = s.getFgi();
                flow_rate = s.getRate();

//                boolean done = f.transmit(transmitted_MBit * flow_rate / total_rate, PConnid); //  why need the id?
                boolean done = f.transmit(transmitted_MBit * flow_rate / total_rate);
                if (done) { // meaning this flowGroup is done.

                    //
                    // wait until GAIA told us to stop, then stop. (although could cause a problem here.)

                    to_remove.add(s);
                }
            }

            for (SubscriptionInfo s : to_remove) {
                total_rate -= s.getRate();
                String fgID = s.getFgi().getID(); // fgID == fgiID

                // remove from two places.
                subscribers.remove(fgID);
                sharedData.subscriptionRateMaps.get(raID).get(pathID).remove(fgID);

                sharedData.finishFlow(fgID);

            }

            // Ensure we don't get rounding errors
            if (subscribers.isEmpty()) {
                total_rate = 0.0;
            }
        }
    }

//    public synchronized void subscribe(FlowInfo f, double rate, long update_ts) {
//        SubscriptionMessage m = new SubscriptionMessage(MsgType.SUBSCRIBE,
//                f, rate, update_ts);
//        try {
//            subscription_queue_.put(m);
//        }
//        catch (InterruptedException e) {
//            e.printStackTrace();
//            System.exit(1);
//        }
//    }
//
//    public synchronized void unsubscribe(FlowInfo f, long update_ts) {
//        SubscriptionMessage m = new SubscriptionMessage(MsgType.UNSUBSCRIBE, f, update_ts);
//
//        try {
//            subscription_queue_.put(m);
//        }
//        catch (InterruptedException e) {
//            e.printStackTrace();
//            System.exit(1);
//        }
//    }
}
