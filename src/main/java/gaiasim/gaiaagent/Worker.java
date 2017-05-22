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

import java.io.BufferedOutputStream;
import java.io.IOException;
import java.net.Socket;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.concurrent.LinkedBlockingQueue;

public class Worker implements Runnable{

//    PConnection conn;
    SharedInterface api;

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
    public volatile double total_rate = 0.0; // TODO(jimmy): track the total_rate.

    public Socket dataSocket;

    // data related

    private final RateLimiter rateLimiter;
    private byte[] data_block = new byte[Constants.BLOCK_SIZE_MB * 1024 * 1024]; // 32MB for now.

    private BufferedOutputStream bos;



    public Worker(String workerID, String RAID, int pathID ,Socket soc, LinkedBlockingQueue<SubscriptionMessage> inputQueue , SharedInterface api){
        this.connID = workerID;
        this.dataSocket = soc;
        this.subcriptionQueue = inputQueue;
        this.api = api;
        this.raID = RAID;
        this.pathID = pathID;

        rateLimiter = RateLimiter.create(Constants.DEFAULT_TOKEN_RATE);

        try {
            bos = new BufferedOutputStream(soc.getOutputStream() , Constants.BUFFER_SIZE );
        } catch (IOException e) {
            e.printStackTrace();
        }

//        data_ = data;
    }

    // TODO: we should not couple the subscription handling and the data sending together!
    @Override
    public void run() {

        // nested while loop
        while (true) {
            SubscriptionMessage m = null;

            // If we don't currently have any subscribers (rate = 0),
            // then data_block until we get some subscription message (take()).
            // Otherwise, check if there's a subscription message, but
            // don't data_block if there isn't one (poll()).
            if (total_rate <= 0.0) {
                try {
                    m = subcriptionQueue.take();
                }
                catch (InterruptedException e) {
                    e.printStackTrace();
                    System.exit(1);
                }
            }
            else {
                m = subcriptionQueue.poll();
            }

            // m will be null only if poll() returned that we have no
            // messages. If m is not null, process the message.
            while (m != null) {

                // handles subscription message.

                // Now we only use the subscription message as a sync signal..

                if (m.getType() == SubscriptionMessage.MsgType.SYNC){
                    // update the worker's subscription info
                    // and go back to work.
                    subscribers.clear();
                    subscribers . putAll( api.subscriptionRateMaps.get(raID).get(pathID) );

                    total_rate = api.subscriptionRateMaps.get(raID).get(pathID).values()
                            .stream().mapToDouble(SubscriptionInfo::getRate).sum();

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

            // If we have some subscribers (rate > 0), then transmit on
            // behalf of the subscribers.
            if (total_rate > 0.0) {
                try {
                    // rate is MBit/s, converting to Block/s

                    double cur_rate = total_rate * Constants.CHEAT_FACTOR_A; // FIXME: also cheat here.

                    int data_length;

                    // check if 100 permits/s is enough (3200MByte/s enough?)
                    if( cur_rate < Constants.BLOCK_SIZE_MB * 8 * Constants.DEFAULT_TOKEN_RATE  ){
                        // no need to change rate , calculate the length
                        rateLimiter.setRate(Constants.DEFAULT_TOKEN_RATE);
                        data_length = (int) (cur_rate / Constants.DEFAULT_TOKEN_RATE * 1024 * 1024 / 8);
                    }
                    else {
                        data_length = Constants.BLOCK_SIZE_MB;
                        rateLimiter.setRate(cur_rate / 8 / Constants.BLOCK_SIZE_MB);
                    }

                    // aquire one permit per flush.
                    rateLimiter.acquire(1);

                    bos.write(data_block , 0, data_length);
                    bos.flush();

//                        System.out.println("PersistentConn: Flushed Writing " + data_length + " w/ rate: " + data_.total_rate + " Mbit/s  @ " + System.currentTimeMillis());
//                    System.out.println("PersistentConn: Flushed Writing w/ rate: " + total_rate + " Mbit/s @ " + System.currentTimeMillis());

                    // distribute transmitted...
                    double tx_ed = (double) data_length * 8 / 1024 / 1024;

                    distribute_transmitted( Constants.CHEAT_FACTOR_B * tx_ed); // FIXME: cheat here!
//                        System.out.println("T_MBit " + tx_ed + " original " + buffer_size_megabits_);
//                        data_.distribute_transmitted(buffer_size_megabits_);
                }
                catch (java.io.IOException e) {
                    e.printStackTrace();
                    System.exit(1);
                }
            }
        } // while (true)
    }


    public synchronized void distribute_transmitted(double transmitted_MBit) {
        if (transmitted_MBit > 0.0 ) {

            ArrayList<SubscriptionInfo> to_remove = new ArrayList<SubscriptionInfo>();

            double flow_rate;
            for (String k : subscribers.keySet()) {
                SubscriptionInfo s = subscribers.get(k);
                FlowGroupInfo f = s.getFgi();
                flow_rate = s.getRate();

//                boolean done = f.transmit(transmitted_MBit * flow_rate / total_rate, PConnid); //  why need the id?
                boolean done = f.transmit(transmitted_MBit * flow_rate / total_rate);
                if (done) { // meaning this flowGroup is done.

                    // TODO: verify this. DO NOTHING here! even if we stop sending, we may still be told to send at a rate by GAIA (because the stale message is in the fly).
                    // wait until GAIA told us to stop, then stop. (although could cause a problem here.)

                    to_remove.add(s);
                }
            }

            // TODO need to remove from subscription list?
            for (SubscriptionInfo s : to_remove) {
                total_rate -= s.getRate();
                String fgID = s.getFgi().getID(); // fgID == fgiID

                // remove from two places.
                subscribers.remove(fgID);
                api.subscriptionRateMaps.get(raID).get(pathID).remove(fgID);
                api.finishFlowGroup(fgID);

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
