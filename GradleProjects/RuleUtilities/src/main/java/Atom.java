public class Atom {

    String relationship = null;
    String variable1 = null;
    String variable2 = null;
    String relationship_name = null;

    public Atom(String relationship, String variable1, String variable2){
        this.relationship = relationship;
        this.variable1 = variable1;
        this.variable2 = variable2;
    }

    public Atom(String relationship, String variable1, String variable2, String relationship_name){
        this.relationship = relationship;
        this.variable1 = variable1;
        this.variable2 = variable2;
        this.relationship_name = relationship_name;
    }


    public String id_print(){
        return this.relationship + "(" + this.variable1 + "," + this.variable2 + ")";
    }

    public String relationship_print(){
        if (this.relationship_name == null){
            return this.relationship + "(" + this.variable1 + "," + this.variable2 + ")";
        }
        return this.relationship_name + "(" + this.variable1 + "," + this.variable2 + ")";
    }

    public String neo4j_print(){
        return "(" + this.variable1 + ")-[:`" + this.relationship + "`]->(" + this.variable2 + ")";
    }


}
