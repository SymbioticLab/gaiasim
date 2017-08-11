package gaiasim.gaiamaster;

import gaiasim.spark.YARNMessages;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;

public class MasterSharedData {

    public volatile ConcurrentHashMap<String , Coflow> coflowPool;

    // index for searching flowGroup in this data structure.
    // only need to add entry, no need to delete entry. TODO verify this.
    public volatile ConcurrentHashMap<String , Coflow> flowIDtoCoflow;

    public volatile boolean flag_CF_ADD = false;
    public volatile boolean flag_CF_FIN = false;
    public volatile boolean flag_FG_FIN = false;

    // move this event queue here because the RPC server module need to access it
    protected LinkedBlockingQueue<YARNMessages> yarnEventQueue = new LinkedBlockingQueue<YARNMessages>();

/*        public AtomicBoolean flag_CF_ADD = new AtomicBoolean(false);
    public AtomicBoolean flag_CF_FIN = new AtomicBoolean(false);
    public AtomicBoolean flag_FG_FIN = new AtomicBoolean(false);*/

    // handles coflow finish.
    public synchronized boolean onFinishCoflow(String coflowID) {
        System.out.println("Master: trying to finish Coflow: " + coflowID);


            // use the get and set method, to make sure that:
            // 1. the value is false before we send COFLOW_FIN
            // 2. the value must be set to true, after whatever we do.
            if( coflowPool.containsKey(coflowID) && !coflowPool.get(coflowID).getAndSetFinished(true) ){

                this.flag_CF_FIN = true;

                coflowPool.remove(coflowID);

//                yarnEventQueue.put(new YARNMessages(coflowID));
                // TODO integrate with YARN, process the returned TRUE.
                return true;
            }

        return false;
    }

    public void addCoflow(String id, Coflow cf){ // trim the co-located flowgroup before adding!
        // first add index
        for ( FlowGroup fg : cf.getFlowGroups().values()){
            flowIDtoCoflow.put( fg.getId() , cf );
        }
        //  then add coflow
        coflowPool.put(id , cf);
    }


    public FlowGroup getFlowGroup(String id){
        if( flowIDtoCoflow.containsKey(id)){
            return flowIDtoCoflow.get(id).getFlowGroup(id);
        }
        else {
            return null;
        }
    }

    // TODO: set the concurrency level.
    public MasterSharedData(){
        this.coflowPool = new ConcurrentHashMap<>();
        this.flowIDtoCoflow = new ConcurrentHashMap<>();
    }

}
