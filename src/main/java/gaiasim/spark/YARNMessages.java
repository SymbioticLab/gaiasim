package gaiasim.spark;

// This is YARNMessages. Intended for communication between the DAGReader and Coflow_Old Inserter.
// We don't need multiple classes for these messages, because they are small and simple.

public class YARNMessages {

    public enum Type{
        DAG_ARRIVAL,
        END_OF_JOBS,
        COFLOW_FIN,
    }
    private Type type;

    // coflow ID for COFLOW_FIN
    public String FIN_coflow_ID;

    // DAG for DAG_ARRIVAL
    public DAG arrivedDAG;

    // default msg: END_OF_JOBS
    public YARNMessages(){
        this.type = Type.END_OF_JOBS;
    }

    public YARNMessages(DAG arrivedDAG){
        this.type = Type.DAG_ARRIVAL;
        this.arrivedDAG = arrivedDAG;
    }

    public YARNMessages(String FIN_coflow_ID){
        this.type = Type.COFLOW_FIN;
        this.FIN_coflow_ID = FIN_coflow_ID;
    }

    public Type getType() { return type; }
}
