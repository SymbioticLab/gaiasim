package gaiasim.spark;

// This is YARNMessages. Intended for communication between the DAGReader and Coflow Inserter.

public class YARNMessages {
    public enum Type{
        DAG_ARRIVAL,
        COFLOW_FIN
    }

    private Type type;
    

    public YARNMessages(Type type){
        this.type = type;

        if (type == Type.DAG_ARRIVAL){

        }
        else if (type == Type.COFLOW_FIN){

        }
    }
}
