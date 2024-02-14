import com.google.common.io.MoreFiles;
import com.google.common.io.RecursiveDeleteOption;
import java.io.*;
import java.util.*;
import java.io.IOException;


@SuppressWarnings("ALL")
public class EvaluateAMIEPatterns {

    public static HashMap<Integer, ArrayList<ArrayList<Integer>>> combine_triples(HashMap<Integer, ArrayList<ArrayList<Integer>>> materialized_triples, HashMap<Integer, ArrayList<ArrayList<Integer>>> train_triples, HashMap<Integer, ArrayList<ArrayList<Integer>>> valid_triples, HashMap<Integer, ArrayList<ArrayList<Integer>>> test_triples) {

        HashMap<Integer, ArrayList<ArrayList<Integer>>> combined_triples = new HashMap<>();
        if (materialized_triples != null) {
            for (int p : materialized_triples.keySet()) {
                ArrayList<ArrayList<Integer>> temp = new ArrayList<>();
                temp.addAll(materialized_triples.get(p));
                if (combined_triples.containsKey(p)) {
                    combined_triples.get(p).addAll(temp);
                } else {
                    combined_triples.put(p, temp);
                }
            }
        }

        if (train_triples != null) {
            for (int p : train_triples.keySet()) {
                ArrayList<ArrayList<Integer>> temp = new ArrayList<>();
                temp.addAll(train_triples.get(p));
                if (combined_triples.containsKey(p)) {
                    combined_triples.get(p).addAll(temp);
                } else {
                    combined_triples.put(p, temp);
                }
            }
        }

        if (valid_triples != null) {
            for (int p : valid_triples.keySet()) {
                ArrayList<ArrayList<Integer>> temp = new ArrayList<>();
                temp.addAll(valid_triples.get(p));
                if (combined_triples.containsKey(p)) {
                    combined_triples.get(p).addAll(temp);
                } else {
                    combined_triples.put(p, temp);
                }
            }
        }

        if (test_triples != null) {
            for (int p : test_triples.keySet()) {
                ArrayList<ArrayList<Integer>> temp = new ArrayList<>();
                temp.addAll(test_triples.get(p));
                if (combined_triples.containsKey(p)) {
                    combined_triples.get(p).addAll(temp);
                } else {
                    combined_triples.put(p, temp);
                }
            }
        }



        return combined_triples;
    }


    public static ArrayList<ArrayList<Integer>> collect_materializations_for_rule(Rule rule, HashMap<Integer, ArrayList<ArrayList<Integer>>> triple_dict){
        Set<ArrayList<Integer>> triples = new HashSet<>();

        for(Atom atom: rule.body_atoms){

            int relationship = Integer.parseInt(atom.relationship);
//            System.out.println(triple_dict.get(relationship).size());
            if(triple_dict.containsKey(relationship))
                triples.addAll(triple_dict.get(relationship));
        }

        int relationship = Integer.parseInt(rule.head_atom.relationship);
//        System.out.println(triple_dict.get(relationship).size());
        if(triple_dict.containsKey(relationship))
            triples.addAll(triple_dict.get(relationship));

        return new ArrayList<>(triples);
    }

    public static boolean check_if_rule_in_test(HashMap<Integer, ArrayList<ArrayList<Integer>>> triples_in_test, Rule r){
        for(Atom atom: r.body_atoms){
            if(!triples_in_test.containsKey(Integer.parseInt(atom.relationship)))
                return false;
        }

        return triples_in_test.containsKey(Integer.parseInt(r.head_atom.relationship));
    }

    public static double calculate_jaccard(Set<String> dataset, Set<String> rules){
        if (dataset.size() ==0 && rules.size() == 0)
            return 1.0;
        Set<String> intersection = new HashSet<String>(dataset);
        intersection.retainAll(rules);

        Set<String> union = new HashSet<String>(dataset);
        union.addAll(rules);

        double overlap = 0.0;
        if (union.size() != 0)
            overlap = intersection.size()*1.0/union.size();

        return overlap;
    }

    public static double calculate_szymkiewicz(Set<String> dataset, Set<String> rules){
        if (dataset.size() ==0 && rules.size() == 0)
            return 1.0;
        Set<String> intersection = new HashSet<String>(dataset);
        intersection.retainAll(rules);

        double overlap = 0.0;
        if (dataset.size() != 0)
            overlap = intersection.size()*1.0/Math.min(dataset.size(), rules.size());
        return overlap;
    }

    public static double calculate_dice(Set<String> dataset, Set<String> rules){
        if (dataset.size() ==0 && rules.size() == 0)
            return 1.0;
        Set<String> intersection = new HashSet<String>(dataset);
        intersection.retainAll(rules);

        double overlap = 0.0;
        if (dataset.size() + rules.size() != 0)
            overlap = 2*intersection.size()*1.0/(dataset.size() + rules.size());
        return overlap;
    }

    public static double calculate_recall(Set<String> dataset, Set<String> rules){
        if (dataset.size() ==0 && rules.size() == 0)
            return 1.0;
        Set<String> tp = new HashSet<String>(dataset);
        tp.retainAll(rules);

        Set<String> fn = new HashSet<String>(dataset);
        fn.removeAll(rules);
        double overlap = 0.0;
        if ((tp.size() + fn.size()) != 0)
            overlap = tp.size()*1.0/(tp.size() + fn.size());

        return overlap;
    }

    public static Set<String> get_symmetric_difference(Set<String> s1, Set<String> s2){
        Set<String> t1 = new HashSet<>(s1);
        Set<String> t2 = new HashSet<>(s2);
        t1.removeAll(s2);
        t2.removeAll(s1);

        Set<String> result = new HashSet<>(t1);
        result.addAll(t2);
        return result;
    }

    public static String identify_inference_pattern(Rule r){

        String pattern = "Unknown";

        if (r.body_atoms.size() == 1) {
            Atom atom1 = r.body_atoms.get(0);
            Atom atom2 = r.head_atom;
            if (atom1.relationship.equals(atom2.relationship)) {
                if (atom1.variable1.equals(atom2.variable2) && atom1.variable2.equals(atom2.variable1)) {
                    pattern = "Symmetry";
                }
            }
            else {
                if (atom1.variable1.equals(atom2.variable1) && atom1.variable2.equals(atom2.variable2)) {
                    pattern = "Hierarchy";
                } else if (atom1.variable1.equals(atom2.variable2) && atom1.variable2.equals(atom2.variable1)) {
                    pattern = "Inversion";
                }
            }
        }
        else{
            Atom atom1 = r.body_atoms.get(0);
            Atom atom2 = r.body_atoms.get(1);
            Atom atom3 = r.head_atom;
            if (!atom1.relationship.equals(atom2.relationship) && !atom2.relationship.equals(atom3.relationship) && !atom1.relationship.equals(atom3.relationship)){
                if(atom1.variable1.equals(atom2.variable1) && atom1.variable1.equals(atom3.variable1) && atom1.variable2.equals(atom2.variable2) && atom1.variable2.equals(atom3.variable2)){
                    pattern = "Intersection";
                }
                else{
                    pattern = "Composition";
                }
            }
            else{
                if (atom1.relationship.equals(atom2.relationship) && atom1.relationship.equals(atom3.relationship)){
                    pattern = "Transitive";
                }
                else{
                    pattern = "Composition";
                }
            }
        }

        return pattern;
    }

    public static void run_rules_get_and_write_metrics(HashMap<Integer, ArrayList<ArrayList<Integer>>> triple_dict_predictions, HashMap<Integer, ArrayList<ArrayList<Integer>>> triple_dict_dataset, HashMap<Integer, ArrayList<ArrayList<Integer>>> triple_dict_dataset_train_valid, BufferedWriter bufferedWriter, String neo4j_dataset, String neo4j_predictions, String neo4j_dataset_train_valid,  List<Rule> filtered_rules, int k, boolean is_asymmetry) throws IOException {
        int rule_ctr = 0;

        bufferedWriter.write("Rule\tInference_pattern\tK\tHC_dataset\tPCA_dataset\tHC_predictions\tPCA_predictions\tHC_adjusted\tPCA_adjusted\t|Support_D|\t|Support_P|\t|Support_D-P|\t|Support_P-D|" +
                "\t|PCA_D|\t|PCA_P|\t|PCA_D-P|\t|PCA_P-D|\tSupport_jaccard\tSupport_dice\tSupport_recall\tSupport_jaccard_filtered\tSupport_dice_filtered\tSupport_recall_filtered" +
                "\tPCA_jaccard\tPCA_dice\tPCA_recall\tPCA_jaccard_filtered\tPCA_dice_filtered\tPCA_recall_filtered\n");
        bufferedWriter.flush();

        for(int i = 0; i<filtered_rules.size(); ++i){
            Rule this_rule = filtered_rules.get(i);
            String pattern = null;
            if (is_asymmetry)
                pattern = "Asymmetry";
            else
                pattern = identify_inference_pattern(this_rule);

            System.out.println("\nProcessing rule " + rule_ctr + "/" + filtered_rules.size() + ": " + this_rule.id_print());
            System.out.println("\tCreating prediction database");
            ArrayList<ArrayList<Integer>> triples = collect_materializations_for_rule(this_rule, triple_dict_predictions);
            NeoClass neo_database_predictions = new NeoClass(neo4j_predictions);
            neo_database_predictions.create_neo4j_database(triples);


            System.out.println("\tCreating original database");
            ArrayList<ArrayList<Integer>> triples_dataset = collect_materializations_for_rule(this_rule, triple_dict_dataset);
            NeoClass neo_database_dataset = new NeoClass(neo4j_dataset);
            neo_database_dataset.create_neo4j_database(triples_dataset);



            System.out.println("\tCreating original database for train and valid");
            ArrayList<ArrayList<Integer>> triples_dataset_train_valid = collect_materializations_for_rule(this_rule, triple_dict_dataset_train_valid);
            NeoClass neo_database_dataset_train_valid = new NeoClass(neo4j_dataset_train_valid);
            neo_database_dataset_train_valid.create_neo4j_database(triples_dataset_train_valid);


            List<Object> result_dataset = null;
            List<Object> result_predictions = null;
            List<Object> result_dataset_train_valid = null;

            if (is_asymmetry){
                System.out.println("\tQuerying original database(Asymmetry)");
                result_dataset= neo_database_dataset.query_rule_asymmetry_memory_efficient(this_rule);
                System.out.println("\tQuerying prediction database(Asymmetry)");
                result_predictions= neo_database_predictions.query_rule_asymmetry_memory_efficient(this_rule);
                System.out.println("\tQuerying original database for train and valid(Asymmetry)");
                result_dataset_train_valid= neo_database_dataset_train_valid.query_rule_asymmetry_memory_efficient(this_rule);
            }
            else {
                System.out.println("\tQuerying original database");
                result_dataset = neo_database_dataset.query_rule_memory_efficient(this_rule);
                System.out.println("\tQuerying prediction database");
                result_predictions = neo_database_predictions.query_rule_memory_efficient(this_rule);
                System.out.println("\tQuerying original database for train and valid");
                result_dataset_train_valid = neo_database_dataset_train_valid.query_rule_memory_efficient(this_rule);
            }

            get_results_and_write(bufferedWriter, this_rule, result_dataset, result_predictions, result_dataset_train_valid, pattern, k, is_asymmetry);
            rule_ctr++;
        }
        bufferedWriter.flush();
    }

    public static List<Rule> get_rules_from_test(List<Rule> rules, HashMap<Integer, ArrayList<ArrayList<Integer>>> triple_dict_dataset_test) {
        List<Rule> filtered_rules = new ArrayList<>();
        for (Rule this_rule: rules){
            if (check_if_rule_in_test(triple_dict_dataset_test, this_rule)){
                filtered_rules.add(this_rule);
            }
        }
        return filtered_rules;
    }

    public static double adjust_metric(double metric_dataset, double metric_predictions, double metric_train_valid){
        double adjusted_metric = 0.0;
        adjusted_metric = (Math.abs(metric_predictions-metric_dataset)*1.0)/(metric_train_valid+metric_dataset);

        return adjusted_metric;
    }

    public static void get_results_and_write(BufferedWriter bufferedWriter, Rule this_rule, List<Object> result_dataset, List<Object> result_predictions, List<Object> result_dataset_train_valid, String pattern, int k, boolean is_asymmetry) throws IOException {


        Set<String> result_dataset_filtered_support = new HashSet((Set<String>) result_dataset.get(0));
        Set<String> result_predictions_filtered_support = new HashSet((Set<String>) result_predictions.get(0));

        result_dataset_filtered_support.removeAll((Set<String>) result_dataset_train_valid.get(0));
        result_predictions_filtered_support.removeAll((Set<String>) result_dataset_train_valid.get(0));


        System.out.println("\tCalculating support overlap");

        double support_overlap_jaccard_adjusted = calculate_jaccard(result_dataset_filtered_support, result_predictions_filtered_support);
        double support_overlap_dice_adjusted = calculate_dice(result_dataset_filtered_support, result_predictions_filtered_support);
        double support_overlap_recall_adjusted = calculate_recall(result_dataset_filtered_support, result_predictions_filtered_support);

        double support_overlap_jaccard = calculate_jaccard((Set<String>) result_dataset.get(0), (Set<String>) result_predictions.get(0));
        double support_overlap_dice = calculate_dice((Set<String>) result_dataset.get(0), (Set<String>) result_predictions.get(0));
        double support_overlap_recall = calculate_recall((Set<String>) result_dataset.get(0), (Set<String>) result_predictions.get(0));

        int len_support_D = result_dataset_filtered_support.size();
        int len_support_P = result_predictions_filtered_support.size();

        Set<String> result_dataset_filtered_support_copy = new HashSet<>(result_dataset_filtered_support);
        Set<String> result_predictions_filtered_support_copy = new HashSet<>(result_predictions_filtered_support);
        result_dataset_filtered_support_copy.removeAll(result_predictions_filtered_support);
        result_predictions_filtered_support_copy.removeAll(result_dataset_filtered_support);
        int len_support_D_P = result_dataset_filtered_support_copy.size();
        int len_support_P_D = result_predictions_filtered_support_copy.size();

        System.out.println("\tCalculating PCA overlap");
        Set<String> result_dataset_filtered_pca = new HashSet((Set<String>) result_dataset.get(1));
        Set<String> result_predictions_filtered_pca = new HashSet((Set<String>) result_predictions.get(1));
        Set<String> result_dataset_train_valid_filtered_pca = new HashSet((Set<String>) result_dataset_train_valid.get(1));

        result_dataset_filtered_pca.removeAll((Set<String>) result_dataset.get(0));
        result_predictions_filtered_pca.removeAll((Set<String>) result_predictions.get(0));
        result_dataset_train_valid_filtered_pca.removeAll((Set<String>) result_dataset_train_valid.get(0));

        if (is_asymmetry){
            Set<String> temp_dataset = new HashSet<>(result_dataset_train_valid_filtered_pca);
            Set<String> temp_predictions = new HashSet<>(result_dataset_train_valid_filtered_pca);
            temp_dataset.removeAll(result_dataset_filtered_pca);
            temp_predictions.removeAll(result_predictions_filtered_pca);
            result_dataset_filtered_pca = temp_dataset;
            result_predictions_filtered_pca = temp_predictions;
        }
        else {
            result_dataset_filtered_pca.removeAll((Set<String>) result_dataset_train_valid.get(1));
            result_predictions_filtered_pca.removeAll((Set<String>) result_dataset_train_valid.get(1));
        }

        System.out.println("\tSize of dataset: " + ((Set<String>)result_dataset.get(0)).size());
        System.out.println("\tSize of predictions: " + ((Set<String>)result_predictions.get(0)).size());
        System.out.println("\tSize of dataset PCA: " + ((Set<String>)result_dataset.get(1)).size());
        System.out.println("\tSize of predictions PCA: " + ((Set<String>)result_predictions.get(1)).size());
        System.out.println("\tSize of dataset PCA after filtering: " + result_dataset_filtered_pca.size());
        System.out.println("\tSize of predictions PCA after filtering: " + result_predictions_filtered_pca.size());

        double pca_overlap_jaccard_adjusted = calculate_jaccard(result_dataset_filtered_pca, result_predictions_filtered_pca);
        double pca_overlap_dice_adjusted = calculate_dice(result_dataset_filtered_pca, result_predictions_filtered_pca);
        double pca_overlap_recall_adjusted = calculate_recall(result_dataset_filtered_pca, result_predictions_filtered_pca);

        double pca_overlap_jaccard = calculate_jaccard((Set<String>) result_dataset.get(1), (Set<String>) result_predictions.get(1));
        double pca_overlap_dice = calculate_dice((Set<String>) result_dataset.get(1), (Set<String>) result_predictions.get(1));
        double pca_overlap_recall = calculate_recall((Set<String>) result_dataset.get(1), (Set<String>) result_predictions.get(1));

        Set<String> pca_dataset_set = (Set<String>) result_dataset.get(1);
        Set<String> pca_predictions_set = (Set<String>) result_predictions.get(1);
        int len_pca_D = pca_dataset_set.size();
        int len_pca_P = pca_predictions_set.size();

        Set<String> pca_dataset_set_copy = new HashSet<>(result_dataset_filtered_pca);
        Set<String> pca_predictions_set_copy = new HashSet<>(result_predictions_filtered_pca);
        pca_dataset_set_copy.removeAll(result_predictions_filtered_pca);
        pca_predictions_set_copy.removeAll(result_dataset_filtered_pca);
        int len_pca_D_P = pca_dataset_set_copy.size();
        int len_pca_P_D = pca_predictions_set_copy.size();

        double hc_train_valid = (double) result_dataset_train_valid.get(2);
        double hc_dataset = (double) result_dataset.get(2);
        double hc_predictions = (double) result_predictions.get(2);
        double pca_train_valid = (double) result_dataset_train_valid.get(3);
        double pca_dataset = (double) result_dataset.get(3);
        double pca_predictions = (double) result_predictions.get(3);

        double hc_adjusted = adjust_metric(hc_dataset, hc_predictions, hc_train_valid);
        double pca_adjusted = adjust_metric(pca_dataset, pca_predictions, pca_train_valid);


        bufferedWriter.write(this_rule.id_print() + "\t" + pattern + "\t" + k + "\t" + hc_dataset + "\t" + pca_dataset + "\t" + hc_predictions + "\t" + pca_predictions + "\t" + hc_adjusted + "\t" + pca_adjusted
                + "\t" + len_support_D + "\t" + len_support_P + "\t" + len_support_D_P + "\t" + len_support_P_D
                + "\t" + len_pca_D + "\t" + len_pca_P + "\t" + len_pca_D_P + "\t" + len_pca_P_D
                + "\t" + support_overlap_jaccard + "\t" + support_overlap_dice + "\t" + support_overlap_recall + "\t" + support_overlap_jaccard_adjusted + "\t" + support_overlap_dice_adjusted + "\t" + support_overlap_recall_adjusted
                + "\t" + pca_overlap_jaccard + "\t" + pca_overlap_dice + "\t" + pca_overlap_recall + "\t" + pca_overlap_jaccard_adjusted + "\t" + pca_overlap_dice_adjusted + "\t" + pca_overlap_recall_adjusted + "\n");
        bufferedWriter.flush();
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
        String path_to_neo4j_database_folder = path_to_neo4j_folder + "/" + dataset_name + "/" + model_name + "/" + dataset_name + "_" + model_name + "_filtered_" + k + "/db_split_" + split_id;
        String path_to_dataset_folder = path_to_data_folder + "/Datasets";

        System.out.println("Dataset name: " + dataset_name + "; Model name: " + model_name);
        String materialization_file_path = path_to_materialization_folder + "/" + dataset_name + "/" + model_name +"/" + dataset_name + "_" + model_name + "_" + split_id + "_filtered_" + k + ".tsv"  ;
        String rules_file_path = path_to_dataset_folder + "/" + dataset_name + "/" +  dataset_name + "_rules.tsv";
        String output_file_path = path_to_rules_folder + "/" + dataset_name + "/" + model_name + "/" +  dataset_name + "_" + model_name + "_evaluated_inference_patterns_" + k +  "_split_" + split_id +  ".tsv";
        String train_triples_path = path_to_dataset_folder + "/" + dataset_name + "/train2id.txt";
        String valid_triples_path = path_to_dataset_folder + "/" + dataset_name + "/valid_" + split_id + "_2id.txt";
        String test_triples_path = path_to_dataset_folder + "/" + dataset_name + "/test_" + split_id + "_2id.txt";

        AMIERuleParser rp = new AMIERuleParser(rules_file_path, path_to_dataset_folder + "/" , model_name, dataset_name, "\t");
        rp.parse_rules_from_file(beta);

        TripleLoader materializations = new TripleLoader(materialization_file_path, false, true);
        TripleLoader train_triples = new TripleLoader(train_triples_path, true, false);
        TripleLoader valid_triples = new TripleLoader(valid_triples_path, true, false);
        TripleLoader test_triples = new TripleLoader(test_triples_path, true, false);

        System.out.println("Materializations: " + materializations.get_triple_count());
        System.out.println("Train triples: " + train_triples.get_triple_count());
        System.out.println("Valid triples: " + valid_triples.get_triple_count());
        System.out.println("Test triples: " + test_triples.get_triple_count());

        HashMap<Integer, ArrayList<ArrayList<Integer>>> triple_dict_predictions = combine_triples(materializations.triple_dict, train_triples.triple_dict, valid_triples.triple_dict, null);
        HashMap<Integer, ArrayList<ArrayList<Integer>>> triple_dict_dataset = combine_triples(null, train_triples.triple_dict, valid_triples.triple_dict, test_triples.triple_dict);
        HashMap<Integer, ArrayList<ArrayList<Integer>>> triple_dict_dataset_test = combine_triples(null, null, null, test_triples.triple_dict);
        HashMap<Integer, ArrayList<ArrayList<Integer>>> triple_dict_dataset_train_valid = combine_triples(null, train_triples.triple_dict, valid_triples.triple_dict, null);


        FileWriter fileWriter = new FileWriter(output_file_path);
        BufferedWriter bufferedWriter = new BufferedWriter(fileWriter);

        String neo4j_dataset = path_to_neo4j_database_folder + "_dataset/";
        String neo4j_predictions = path_to_neo4j_database_folder + "_predictions/";
        String neo4j_dataset_train_valid = path_to_neo4j_database_folder + "_dataset_train_valid/";

        List<Rule> filtered_rules = get_rules_from_test(rp.rules, triple_dict_dataset_test);
        System.out.println("Number of rules(regular) originally: " + rp.rules.size());
        System.out.println("Number of rules(regular) after filtering: " + filtered_rules.size());
        run_rules_get_and_write_metrics(triple_dict_predictions, triple_dict_dataset, triple_dict_dataset_train_valid, bufferedWriter, neo4j_dataset, neo4j_predictions, neo4j_dataset_train_valid, filtered_rules, k, false);

        File neo4j_folder_dataset = new File(neo4j_dataset);
        MoreFiles.deleteRecursively(neo4j_folder_dataset.toPath(), RecursiveDeleteOption.ALLOW_INSECURE);
        File neo4j_folder_predictions = new File(neo4j_predictions);
        MoreFiles.deleteRecursively(neo4j_folder_predictions.toPath(), RecursiveDeleteOption.ALLOW_INSECURE);
        File neo4j_folder_dataset_train_valid = new File(neo4j_dataset_train_valid);
        MoreFiles.deleteRecursively(neo4j_folder_dataset_train_valid.toPath(), RecursiveDeleteOption.ALLOW_INSECURE);
        System.out.println("Finished DatasetPredictionOverlap");

        bufferedWriter.close();
    }

    }