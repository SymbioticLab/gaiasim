package gaiasim.agent;

import java.util.ArrayList;
import java.util.HashMap;

import gaiasim.agent.Connection;
import gaiasim.agent.SendingAgent;

// Information about a flow as tracked by a sending agent.
public class FlowInfo {
    public class PendingSubscription {
        public Connection conn_;
        public double rate_;
        public PendingSubscription(Connection conn, double rate) {
            conn_ = conn;
            rate_ = rate;
        }
    }
    public String id_;
    public double volume_;
    public double transmitted_;
    public int num_subflows_;
    public SendingAgent.Data sa_;
    public ArrayList<PendingSubscription> pending_subscriptions_ = 
        new ArrayList<PendingSubscription>();
    public HashMap<String, Connection> subscriptions_ = new HashMap<String, Connection>();
    public volatile boolean done_ = false;

    // Flag to check to determine whether we should send a FIN
    // for this flow immediatly upon completion. If the flow's
    // status has just been reported to the controller, and the
    // sending agent completes the flow before receiving an
    // update from the controller, the flag is set to true. This
    // informs the sending agent to wait until it receives an
    // update for the flow to send the FIN message. If the flag
    // is set to false, it is safe to send a FIN.
    public volatile boolean update_pending_ = false;

    public FlowInfo(String id, int num_subflows, double volume, SendingAgent.Data sa) {
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
    public synchronized void add_subflow(Connection c, double rate) {
        if (pending_subscriptions_.size() + 1 == num_subflows_) {

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
                c.data_.subscribe(this, rate);
                subscriptions_.put(c.data_.id_, c);

                for (PendingSubscription s : pending_subscriptions_) {
                    s.conn_.data_.subscribe(this, s.rate_);
                    subscriptions_.put(s.conn_.data_.id_, s.conn_);
                }

                pending_subscriptions_.clear();
            }
        }
        else {
            pending_subscriptions_.add(new PendingSubscription(c, rate));
        }

    }

    public synchronized void set_update_pending(boolean val) {
        update_pending_ = val;
    }

    // Returns true if the flow is done and false otherwise.
    // After transmitting some amount on behalf of a flow, a Connection
    // will call this function to update the amount of flow transmitted.
    // To avoid deadlocks and races, a completed flow will not unsubscribe
    // itself. Rather, each Connection will remove the flow's subscription
    // upon calling this function and finding a flow complete. Yes, this
    // this means that some extra data might be transmitted, but this
    // should only be a small amount.
    public synchronized boolean transmit(double transmitted, String conn_id) {

        // If the flow is already done, remove our this connection from the flow.
        // NOTE: Could have just incremented transmitted_ and checked against
        //       volume_, but doing so could cause overflow in the case where
        //       where more than one connection is adding its transmitted amount.
        if (done_) {
            subscriptions_.remove(conn_id);

            // We know that all connections have recognized this flow as complete
            // once the subscriptions are empty. At that point we may remove the flow.
            // TODO: Consider sending the FIN message after the first conneciton
            //       recognizes completion, and simply delay flow deletion. We want
            //       the FIN to reach the controller as fast as possible, but to
            //       delay deletion until it is safe to do so.
            if (!update_pending_ && subscriptions_.isEmpty()) {
                sa_.finish_flow(id_);
            }
            else {
                System.out.println(id_ + " subflow finished, but not sending FIN. update_pending_=" + update_pending_ + " subs.size()=" + subscriptions_.size());
            }

            return true;
        }

        transmitted_ += transmitted;
        if (transmitted_ >= volume_) {
            done_ = true;
            subscriptions_.remove(conn_id);

            if (!update_pending_ && subscriptions_.isEmpty()) {
                sa_.finish_flow(id_);
            }
            else {
                System.out.println(id_ + " subflow finished, but not sending FIN. update_pending_=" + update_pending_ + " subs.size()=" + subscriptions_.size());
            }

            
            return true;
        } // transmitted_ >= volume_

        return false;
    }

    public synchronized void update_flow(int num_subflows, double volume) {
        volume_ = volume;
        num_subflows_ = num_subflows;
        for (String k : subscriptions_.keySet()) {
            Connection c = subscriptions_.get(k);
            c.data_.unsubscribe(id_);    
        }
        subscriptions_.clear();
        pending_subscriptions_.clear();

        update_pending_ = false;
    }

}
