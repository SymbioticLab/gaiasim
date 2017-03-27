package gaiasim.agent;

import java.util.ArrayList;

import gaiasim.agent.Connection;

// Information about a flow as tracked by a sending agent.
public class FlowInfo {
    public String id_;
    public double volume_;
    public double transmitted_;
    public ArrayList<Connection> subscriptions_;
    public boolean done_ = false;

    public void FlowInfo(String id, double volume) {
        id_ = id;
        volume_ = volume;
    }

    public synchronized void add_subflow(Connection c, double rate) {
        c.subscribe(this, rate);
        subscriptions_.add(c);
    }

    // Returns true if the flow is done and false otherwise.
    // After transmitting some amount on behalf of a flow, a Connection
    // will call this function to update the amount of flow transmitted.
    // To avoid deadlocks and races, a completed flow will not unsubscribe
    // itself. Rather, each Connection will remove the flow's subscription
    // upon calling this function and finding a flow complete. Yes, this
    // this means that some extra data might be transmitted, but this
    // should only be a small amount.
    public synchronized boolean transmit(double transmitted, Connection conn) {

        // If the flow is already done, remove our this connection from the flow.
        // NOTE: Could have just incremented transmitted_ and checked against
        //       volume_, but doing so could cause overflow in the case where
        //       where more than one connection is adding its transmitted amount.
        if (done_) {
            subscriptions_.remove(conn);

            // We know that all connections have recognized this flow as complete
            // once the subscriptions are empty. At that point we may remove the flow.
            // TODO: Consider sending the FIN message after the first conneciton
            //       recognizes completion, and simply delay flow deletion. We want
            //       the FIN to reach the controller as fast as possible, but to
            //       delay deletion until it is safe to do so.
            if (subscriptions_.isEmpty()) {
                // TODO: Send a FIN
            }

            return true;
        }

        transmitted_ += transmitted;
        if (transmitted_ >= volume_) {
            done_ = true;
            subscriptions_.remove(conn);

            if (subscriptions_.isEmpty()) {
                // TODO: Send a FIN
            }
            
            return true;
        } // transmitted_ >= volume_

        return false;
    }
}
