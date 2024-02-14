import java.util.List;

public class Rule {
    double standard_confidence = 0.0;
    Atom head_atom = null;
    List<Atom> body_atoms = null;
    double head_coverage = 0.0;
    double pca_confidence = 0.0;
    String functional_variable = null;
    double selectivity = 0.0;

    public Rule(Atom head_atom, List<Atom> body_atoms){
        this.head_atom = head_atom;
        this.body_atoms = body_atoms;
    }

    public Rule(Atom head_atom, List<Atom> body_atoms, String functional_variable){
        this.head_atom = head_atom;
        this.body_atoms = body_atoms;
        this.functional_variable = functional_variable;
    }

    public Rule(Atom head_atom, List<Atom> body_atoms, double head_coverage, double pca_confidence, double standard_confidence, String functional_variable, double beta){

        this.head_atom = head_atom;
        this.body_atoms = body_atoms;
        this.head_coverage = head_coverage;
        this.pca_confidence = pca_confidence;
        this.standard_confidence = standard_confidence;
        this.functional_variable = functional_variable;
        this.selectivity = ((1 + beta * beta) * this.pca_confidence * this.head_coverage) / (
                beta * beta * this.pca_confidence + this.head_coverage);
    }

    public String id_print(){
        StringBuilder rule_to_string = new StringBuilder();

        for(Atom atom: this.body_atoms){
            rule_to_string.append(atom.id_print()).append(" ");
        }

        rule_to_string.append("==> ").append(this.head_atom.id_print());

        return rule_to_string.toString();
    }

    public String relationship_print(){
        StringBuilder rule_to_string = new StringBuilder();

        for(Atom atom: this.body_atoms){
            rule_to_string.append(atom.relationship_print()).append(" ");
        }

        rule_to_string.append("==> ").append(this.head_atom.relationship_print());

        return rule_to_string.toString();
    }

}
