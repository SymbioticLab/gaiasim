package gaiasim.gaiamaster;

import gaiasim.spark.YARNMessages;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;

public class MasterSharedData {
    private static final Logger logger = LogManager.getLogger();

    volatile ConcurrentHashMap<String , Coflow> coflowPool;

    // index for searching flowGroup in this data structure.
    // only need to add entry, no need to delete entry. TODO verify this.
    private volatile ConcurrentHashMap<String , Coflow> flowIDtoCoflow;

    volatile boolean flag_CF_ADD = false;
    volatile boolean flag_CF_FIN = false;
    volatile boolean flag_FG_FIN = false;

    // move this event queue here because the RPC server module need to access it
    protected LinkedBlockingQueue<YARNMessages> yarnEventQueue = new LinkedBlockingQueue<YARNMessages>();

/*        public AtomicBoolean flag_CF_ADD = new AtomicBoolean(false);
    public AtomicBoolean flag_CF_FIN = new AtomicBoolean(false);
    public AtomicBoolean flag_FG_FIN = new AtomicBoolean(false);*/

// stats
    int flowStartCnt = 0;
    int flowFINCnt = 0;

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

    public void onFinishFlowGroup(String fid, long timestamp) {

        flowFINCnt ++;

        FlowGroup fg = getFlowGroup(fid);
        if (fg == null){
            logger.warn("fg == null for fid = {}", fid);
            return;
        }
        if(fg.getAndSetFinish(timestamp)){
            return; // if already finished, do nothing.
        }

        flag_FG_FIN = true;

        // check if the owning coflow is finished
        Coflow cf = coflowPool.get(fg.getOwningCoflowID());

        if(cf == null){ // cf may already be finished.
            return;
        }

        boolean flag = true;

        // TODO verify concurrency issues here. here cf may be null.
        for(FlowGroup ffg : cf.getFlowGroups().values()){
            flag = flag && ffg.isFinished();
        }

        // if so set coflow status, send COFLOW_FIN
        if (flag){
            String coflowID = fg.getOwningCoflowID();
            if ( onFinishCoflow(coflowID) ){
                try {
                    yarnEventQueue.put(new YARNMessages(coflowID));
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }
    }
}
