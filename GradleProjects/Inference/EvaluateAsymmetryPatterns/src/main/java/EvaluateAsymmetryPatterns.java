import com.google.common.io.MoreFiles;
import com.google.common.io.RecursiveDeleteOption;
import java.io.*;
import java.util.*;
import java.io.IOException;


@SuppressWarnings("ALL")
public class EvaluateAsymmetryPatterns {


    public static HashMap<String, String> get_relation_to_functional_variable(AMIERuleParser rp){
        HashMap<String, String> relation_to_functional_variable = new HashMap<>();
        for (Rule rule: rp.rules){
            String functional_variable = rule.functional_variable;
            relation_to_functional_variable.put(rule.head_atom.relationship, functional_variable.replace("?",""));
        }
        return relation_to_functional_variable;
    }


    public static void main(String[] args) throws IOException {
        String path_to_data_folder = args[0];
        String dataset_name = args[1];
        String model_name = args[2];
        int k = Integer.parseInt(args[3]);
        double beta = Double.parseDouble(args[4]);
        String split_id = args[5];

        String path_to_materialization_folder = path_to_data_folder + "/Materializations";
        String path_to_rules_folder = path_to_data_folder + "/MinedRules";
        String path_to_neo4j_folder = path_to_data_folder + "/db";
        String path_to_neo4j_database_folder = path_to_neo4j_folder + "/" + dataset_name + "/" + model_name + "/" + dataset_name + "_" + model_name + "_filtered_asymmetry_" + k + "/db_split_" + split_id;
        String path_to_dataset_folder = path_to_data_folder + "/Datasets";

        System.out.println("Dataset name: " + dataset_name + "; Model name: " + model_name);
        String materialization_file_path = path_to_materialization_folder + "/" + dataset_name + "/" + model_name +"/" + dataset_name + "_" + model_name + "_" + split_id + "_filtered_" + k + ".tsv"  ;
        String rules_file_path = path_to_dataset_folder + "/" + dataset_name + "/" +  dataset_name + "_rules.tsv";
        String output_file_path_asymmetry = path_to_rules_folder + "/" + dataset_name + "/" + model_name + "/" +  dataset_name + "_" + model_name + "_evaluated_inference_patterns_asymmetry_" + k +  "_split_" + split_id +  ".tsv";

        String train_triples_path = path_to_dataset_folder + "/" + dataset_name + "/train2id.txt";
        String valid_triples_path = path_to_dataset_folder + "/" + dataset_name + "/valid_" + split_id + "_2id.txt";
        String test_triples_path = path_to_dataset_folder + "/" + dataset_name + "/test_" + split_id + "_2id.txt";

        AMIERuleParser rp = new AMIERuleParser(rules_file_path, path_to_dataset_folder + "/" , model_name, dataset_name, "\t");
        rp.parse_rules_from_file(beta);

        TripleLoader materializations = new TripleLoader(materialization_file_path, false, true);
        TripleLoader train_triples = new TripleLoader(train_triples_path, true, false);
        TripleLoader valid_triples = new TripleLoader(valid_triples_path, true, false);
        TripleLoader test_triples = new TripleLoader(test_triples_path, true, false);

        HashMap<Integer, ArrayList<ArrayList<Integer>>> triple_dict_predictions = EvaluateAMIEPatterns.combine_triples(materializations.triple_dict, train_triples.triple_dict, valid_triples.triple_dict, null);
        HashMap<Integer, ArrayList<ArrayList<Integer>>> triple_dict_dataset = EvaluateAMIEPatterns.combine_triples(null, train_triples.triple_dict, valid_triples.triple_dict, test_triples.triple_dict);
        HashMap<Integer, ArrayList<ArrayList<Integer>>> triple_dict_dataset_test = EvaluateAMIEPatterns.combine_triples(null, null, null, test_triples.triple_dict);
        HashMap<Integer, ArrayList<ArrayList<Integer>>> triple_dict_dataset_train_valid = EvaluateAMIEPatterns.combine_triples(null, train_triples.triple_dict, valid_triples.triple_dict, null);


        FileWriter fileWriterA = new FileWriter(output_file_path_asymmetry);
        BufferedWriter bufferedWriterA = new BufferedWriter(fileWriterA);

        String neo4j_dataset = path_to_neo4j_database_folder + "_dataset/";
        String neo4j_predictions = path_to_neo4j_database_folder + "_predictions/";
        String neo4j_dataset_train_valid = path_to_neo4j_database_folder + "_dataset_train_valid/";


        HashMap<String, String> relation_to_functional_variable = get_relation_to_functional_variable(rp);

        int n_relations = rp.id_to_relationship.size();
        System.out.println("Number of relations: " + n_relations);
        List<Rule> asymmetry_rules = new ArrayList<>();
        for (int i = 0; i < n_relations; i++) {
            List<Atom> body_atoms = new ArrayList<>();
            body_atoms.add(new Atom("" + i, "a", "b", rp.id_to_relationship.get("" + i)));
            Atom head_atom = new Atom("" + i, "b", "a", rp.id_to_relationship.get("" + i));
            String functional_variable = relation_to_functional_variable.getOrDefault(rp.id_to_relationship.get("" + i), "a");
            Rule r = new Rule(head_atom, body_atoms, 0.1,0.1,0.1, functional_variable, 1.0);
            asymmetry_rules.add(r);
        }
        List<Rule> filtered_rules_asymmetry = EvaluateAMIEPatterns.get_rules_from_test(asymmetry_rules, triple_dict_dataset_test);
        System.out.println("Number of rules(asymmetry) originally: " + asymmetry_rules.size());
        System.out.println("Number of rules(asymmetry) after filtering: " + filtered_rules_asymmetry.size());

        EvaluateAMIEPatterns.run_rules_get_and_write_metrics(triple_dict_predictions, triple_dict_dataset, triple_dict_dataset_train_valid, bufferedWriterA, neo4j_dataset, neo4j_predictions, neo4j_dataset_train_valid, filtered_rules_asymmetry, k, true);


        File neo4j_folder_dataset = new File(neo4j_dataset);
        MoreFiles.deleteRecursively(neo4j_folder_dataset.toPath(), RecursiveDeleteOption.ALLOW_INSECURE);
        File neo4j_folder_predictions = new File(neo4j_predictions);
        MoreFiles.deleteRecursively(neo4j_folder_predictions.toPath(), RecursiveDeleteOption.ALLOW_INSECURE);
        File neo4j_folder_dataset_train_valid = new File(neo4j_dataset_train_valid);
        MoreFiles.deleteRecursively(neo4j_folder_dataset_train_valid.toPath(), RecursiveDeleteOption.ALLOW_INSECURE);
        System.out.println("Finished DatasetPredictionOverlap");

        bufferedWriterA.close();

    }

}