package gaiasim.spark;

// This is essentially the DAG class from the emulator, implemented as a state machine here to track dependency.

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.SetMultimap;
import gaiasim.network.Coflow;
import gaiasim.network.Flow;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;

public class DependencyResolver {
    public final String id;

    public HashMap<String, Coflow> coflowList; // we don't remove coflows from this list while executing.

    //// ******* We remove coflows from the {rootCoflowsID, to_parents } while executing.
    public HashSet<String> rootCoflowsID; // root Coflows := active coflows // MUST use set to guarantee uniqueness.


    // using SetMultimap so there will be no duplicate items.
    // This is actually mapping between stages, so contains more than mapping between coflows.
    public SetMultimap<String, String> coflow_to_parents;
    public SetMultimap<String, String> coflow_to_children; // we also don't remove coflows from this list.

    public DependencyResolver(String id) {
        this.id = id;
        this.rootCoflowsID = new HashSet<String>();
        this.coflowList = new HashMap<>();
        this.coflow_to_parents = HashMultimap.create();
        this.coflow_to_children = HashMultimap.create();
    }

    public boolean isDone() {
        return rootCoflowsID.isEmpty();
    }

    // Given {src, dst}, set the appropriate dependency:
    // (1) dst.parent.has(src)
    // (2) src.child.has(dst)
    public void updateDependency(String src_stage, String dst_stage) {
        // first set the coflow -> [] parent mapping
        coflow_to_parents.put(this.id + ":" + dst_stage, this.id + ":" + src_stage);
        // then set the coflow -> children mapping
        coflow_to_children.put(this.id + ":" + src_stage, this.id + ":" + dst_stage);

    }

    // construct a list of coflow from the Multimap
    // This is where the Coflow objects are initiated.
    public void addCoflows(ArrayListMultimap<String, Flow> tmpCoflowList) {
        for (String coflowID : tmpCoflowList.keySet()) {

            Coflow cf = new Coflow(coflowID, null); // task_locs not used in the future

            // iterate through all the contained Flow(Group)s
            for (Flow f : tmpCoflowList.get(coflowID)) {
                cf.flows_.put(f.id_, f);
                cf.volume_ += f.volume_;
            }

            coflowList.put(coflowID, cf);
        }

        // set the coflow dependency, only after we are done creating all Coflow instances
        for (Map.Entry<String, Coflow> entry : coflowList.entrySet()) {
            String coflowID = entry.getKey();
            Coflow cf = entry.getValue();

            for (String childID : coflow_to_children.get(coflowID)) {
                if (coflowList.containsKey(childID)) { // we need to check if the key maps a valid Coflow
                    cf.child_coflows.add(coflowList.get(childID));
                }
            }
            for (String parentID : coflow_to_parents.get(coflowID)) {
                if (coflowList.containsKey(parentID)) { // we need to check if the key maps a valid Coflow
                    cf.parent_coflows.add(coflowList.get(parentID));
                }
            }
        }
    }

    // parse the dependencies and find the roots.
    // we can ONLY do this after we are done with all coflowList and dependencies!
    public void updateRoot() {
        HashSet<String> rootsToAdd = new HashSet<String>();
        // for each Coflow, check if it has (valid) parents, if not, add to the set.
        for (String k : coflowList.keySet()) {
            // remove the redundant coflow_to_parent entries
            coflow_to_parents.get(k).removeIf(parent -> !coflowList.containsKey(parent));

            // then check if this is an outstanding coflow.
            if (!coflow_to_parents.containsKey(k)) {
                rootsToAdd.add(k);
            }
        }

        rootCoflowsID.addAll(rootsToAdd);
    }

    public ArrayList<Coflow> getRootCoflows() {
        ArrayList<Coflow> ret = new ArrayList<Coflow>(rootCoflowsID.size());
        for (String k : rootCoflowsID) {
            ret.add(coflowList.get(k));
        }
        return ret;
    }

    ///// getters and setters /////
    public String getId() {
        return id;
    }

}
