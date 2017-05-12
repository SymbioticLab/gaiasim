package gaiasim.agent;

import java.util.HashMap;

// Information about a flow as tracked by a sending agent.
public class FlowInfo {
    public class PendingSubscription {
        public PersistentConnection conn_;
        public double rate_;
        public PendingSubscription(PersistentConnection conn, double rate) {
            conn_ = conn;
            rate_ = rate;
        }
    }
    public String id_;
    public double volume_;
    public double transmitted_;
    public int num_subflows_;
    public PersistentSendingAgent.DataBroker sa_;
    public HashMap<String, PendingSubscription> pending_subscriptions_ = 
        new HashMap<String, PendingSubscription>();
    public HashMap<String, PersistentConnection> subscriptions_ = new HashMap<String, PersistentConnection>();
    public volatile boolean done_ = false;

    // If true, the flow has not just performed a mass unsubscribe.
    public volatile boolean subbed_ = false;

    // Flag to check to determine whether we should send a FIN
    // for this flow immediatly upon completion. If the flow's
    // status has just been reported to the controller, and the
    // sending agent completes the flow before receiving an
    // update from the controller, the flag is set to true. This
    // informs the sending agent to wait until it receives an
    // update for the flow to send the FIN message. If the flag
    // is set to false, it is safe to send a FIN.
    public volatile boolean update_pending_ = false;

    // Timestamp of the latest update that happened to this flow.
    // (Un)subscriptions can only be committed if their timestamp
    // matches that of the flow.
    public volatile long update_ts_ = 0;

    public FlowInfo(String id, int num_subflows, double volume, PersistentSendingAgent.DataBroker sa) {
        id_ = id;
        volume_ = volume;
        num_subflows_ = num_subflows;
        sa_ = sa;
    }

    // Called by a sending agent upon receiving a SUBFLOW_INFO message
    // for this flow from the controller. We do not immediately add
    // a subscrition for a subflow in response to a SUBFLOW_INFO message.
    // Rather, we wait until we've received all expected SUBFLOW_INFO
    // messages for this flow (we know how many we're expecting from
    // field0 of the preceding FLOW_{START,UPDATE}). This way all subflows
    // will start at roughly the same time, so we don't have to worry
    // about an earlier-starting subflow completing a small flow before
    // all other subflows have been added.
    public synchronized void add_subflow(PersistentConnection c, double rate) {
        pending_subscriptions_.put(c.data_.id_, new PendingSubscription(c, rate));
        if (pending_subscriptions_.size() == num_subflows_) {

            // If the flow was finished while we were waiting for
            // an update, we should now send the FIN message for this flow.
            // We should wait until we've received all subflow updates
            // before sending the FIN so that we don't remove the flow
            // from the sending agent's flow_table when there are still
            // potentially in-flight updates for the flow (could result
            // in a null-access to the table).
            if (done_) {
                sa_.finish_flow(id_); 
            }
            else {
                for (String k : pending_subscriptions_.keySet()) {
                    PendingSubscription s = pending_subscriptions_.get(k);
                    s.conn_.data_.subscribe(this, s.rate_, update_ts_);
                }
            }

            update_pending_ = false;
        }
    }

    // Returns true if the PersistentConnection was able to commit this FlowInfo's
    // subscription, false otherwise.
    public synchronized boolean commit_subscription(String conn_id, long ts) {
        if (done_ || ts != update_ts_) {
            return false;
        }
        
        subscriptions_.put(conn_id, pending_subscriptions_.get(conn_id).conn_);
        pending_subscriptions_.remove(conn_id);
        return true;
    }

    // Returns true if the PersistentConnection was able to commit this FlowInfo's
    // unsubscription, false otherwise.
    public synchronized boolean commit_unsubscription(String conn_id, long ts) {
        // NOTE: Potentially need to be careful about overflow here
        if (done_ || ts != update_ts_ - 1) {
            return false;
        }
        
        subscriptions_.remove(conn_id);
        return true;
    }

    public synchronized void set_update_pending(boolean val) {
        update_pending_ = val;
    }

    // Returns true if the flow is done and false otherwise.
    // After transmitting some amount on behalf of a flow, a PersistentConnection
    // will call this function to update the amount of flow transmitted.
    // To avoid deadlocks and races, a completed flow will not unsubscribe
    // itself. Rather, each PersistentConnection will remove the flow's subscription
    // upon calling this function and finding a flow complete. Yes, this
    // this means that some extra data might be transmitted, but this
    // should only be a small amount.
    public synchronized boolean transmit(double transmitted, String conn_id) {

        // If the flow is already done, remove our this connection from the flow.
        // NOTE: Could have just incremented transmitted_volume and checked against
        //       volume_, but doing so could cause overflow in the case where
        //       where more than one connection is adding its transmitted amount.
        if (done_) {
            subscriptions_.remove(conn_id);
            return true;
        }

        transmitted_ += transmitted;
        // Check if we're the first connection to reconginze this flow as completed.
        // We know that we are because done set within this function and this
        // is a synchronized function. If some other connection completed the flow
        // before we did, then it would have set done to true before we had called
        // this function, and we would've hit the if(done) condition at the
        // beginning of this function.
        if (transmitted_ >= volume_) {
            done_ = true;
            subscriptions_.remove(conn_id);

            if (!update_pending_) {
                sa_.finish_flow(id_);
            }
            
            return true;
        } // transmitted_volume >= volume_

        return false;
    }

    public synchronized void update_flow(int num_subflows, double volume) {
        volume_ = volume;
        num_subflows_ = num_subflows;

        if (!done_) {
            for (String k : subscriptions_.keySet()) {
                PersistentConnection c = subscriptions_.get(k);
                c.data_.unsubscribe(this, update_ts_);    
            }
           
            update_ts_++;
    
        }

        // If num_subflows is being set to 0, then the controller has scheduled
        // this flow not to run currently. However, while the controller was
        // making its scheduling decision, we may have completed the flow. If
        // this is the case, send a FIN back to the controller for this flow.
        if (num_subflows_ == 0 && done_) {
            sa_.finish_flow(id_);
            update_pending_ = false;
        }

        pending_subscriptions_.clear();
    }
}
