
import java.io.File;
import java.io.FileNotFoundException;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Scanner;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import org.jgrapht.Graph;
import org.jgrapht.GraphPath;
import org.jgrapht.alg.shortestpath.DijkstraShortestPath;
import org.jgrapht.graph.DefaultEdge;
import org.jgrapht.graph.DirectedPseudograph;

import org.apache.commons.math3.stat.descriptive.DescriptiveStatistics;

import com.google.common.collect.HashBasedTable;
import com.google.common.collect.Table;

public class ComputeResults {

    private static final String YES_YES = "\\sfrac{\\cmark}{\\cmark}", YES_UNK = "\\sfrac{\\cmark}{--}", YES_NO = "\\sfrac{\\cmark}{\\xmark}", NO_UNK = "\\sfrac{\\xmark}{--}", NO_NO = "\\sfrac{\\xmark}{\\xmark}", UNK_UNK = "\\sfrac{--}{--}";

    public static Map<String, Map<String, String>> get_output(String[] splits_ids, String dataset, String evidence) throws FileNotFoundException {
        Map<String, Map<String, DescriptiveStatistics>> stat_map = new HashMap<>();
        Map<String, Map<String, String>> final_map_mean = new HashMap<>();
        Map<String, Map<String, String>> final_map_sd = new HashMap<>();
        DecimalFormat formatter = new DecimalFormat(".00");

        for(String split_id: splits_ids){
            Map<String, Map<String, List<Double>>> output_for_split = computeResults(split_id, dataset, evidence);
            add_result_to_stats(output_for_split, stat_map);
        }

        for(String pattern: stat_map.keySet()){
            if (!final_map_mean.containsKey(pattern)){
                final_map_mean.put(pattern, new HashMap<>());
            }
            if (!final_map_sd.containsKey(pattern)){
                final_map_sd.put(pattern, new HashMap<>());
            }
            for(String model: stat_map.get(pattern).keySet()){
                final_map_mean.get(pattern).put(model, formatter.format(stat_map.get(pattern).get(model).getMean()));
                final_map_sd.get(pattern).put(model, formatter.format(stat_map.get(pattern).get(model).getStandardDeviation()));
            }
        }

        return final_map_mean;
    }



    public static void main(String[] args) throws FileNotFoundException {

        String dataset = "YAGO3-10";
        Map<String, Map<String, String>> final_map_positive = get_output(new String[]{"0", "1", "2", "3", "4", "5", "6", "7", "8", "9"}, dataset, "Support_dice_filtered");
        Map<String, Map<String, String>> final_map_negative = get_output(new String[]{"0", "1", "2", "3", "4", "5", "6", "7", "8", "9"}, dataset, "PCA_dice_filtered");

        String[] models = new String[]{"boxe", "complex", "hake", "hole", "quate", "rotate", "rotpro", "toruse", "transe"};
        String[] patternsInOrder = new String[] {"Hierarchy", "Symmetry", "Antisymmetry", "Inversion", "Intersection", "Transitive", "Composition"};
        String[] otherTypesInOrder = new String[] {"Gen.~Intersection", "B.~Transitive", "Equality", "B.~Composition", "Commonality",
                "Any F.~Path", "Any B.~Path", "Any Commonality"};

        Table<String, String, String> expected = getExpectedTable();

        StringBuilder sb = new StringBuilder();
        sb.append("Pattern,boxe,complex,hake,hole,quate,rotate,rotpro,toruse,transe\n");
        for(String pattern: patternsInOrder){
            if (!final_map_positive.containsKey(pattern) || !final_map_negative.containsKey(pattern)){
                continue;
            }
            sb.append(" & ").append(pattern);
            for(String model: models){
//                sb.append(" & ").append(final_map_positive.get(pattern).get(model)).append(" (").append(final_map_negative.get(pattern).get(model)).append(")").append(" \\quad ").append(expected.contains(pattern, model)?expected.get(pattern, model):UNK_UNK).append("");
                sb.append(" & ").append(final_map_positive.get(pattern).get(model)).append(" (").append(final_map_negative.get(pattern).get(model)).append(")");

            }
            sb.append("\\\\ \\cline{2-11} \n");
        }

        sb.append("\n\n");
        sb.append("Pattern,boxe,complex,hake,hole,quate,rotate,rotpro,toruse,transe\n");
        for(String pattern: otherTypesInOrder){
            if (!final_map_positive.containsKey(pattern) || !final_map_negative.containsKey(pattern)){
                continue;
            }
            sb.append(" & ").append(pattern);
            for(String model: models){
                sb.append(" & ").append(final_map_positive.get(pattern).get(model)).append(" (").append(final_map_negative.get(pattern).get(model)).append(")");
//                sb.append(",").append(final_map_mean.get(pattern).get(model));

            }
            sb.append("\\\\ \\cline{2-11} \n");
        }
        System.out.println(sb);
    }


    public static void add_result_to_stats(Map<String, Map<String, List<Double>>> output_for_split, Map<String, Map<String, DescriptiveStatistics>> stat_map){

        for(String pattern: output_for_split.keySet()){
            if (!stat_map.containsKey(pattern)){
                stat_map.put(pattern, new HashMap<>());
            }
            for(String model: output_for_split.get(pattern).keySet()){
                if (!stat_map.get(pattern).containsKey(model)){
                    stat_map.get(pattern).put(model, new DescriptiveStatistics());
                }
                List<Double> results = output_for_split.get(pattern).get(model);
                DescriptiveStatistics stats = new DescriptiveStatistics();
                for(Double result: results){
                    stats.addValue(result);
                }
                stat_map.get(pattern).get(model).addValue(stats.getMean());
            }
        }
    }
    public static Map<String, Map<String, List<Double>>> computeResults(String split_id, String dataset, String evidence) throws FileNotFoundException {
        // Expected: for each pattern and model, what is the expectation.

        Table<String, String, String> expected = getExpectedTable();

        System.out.println("Dataset: " + dataset);

        String folderWithRules = "D:\\PhD\\Work\\Phd_research\\data\\RebuttalMinedRules\\";
                /*dataset = "WN18RR", /*a = "5", b = "10",*/
        String toCollectA = evidence;
        String toCollectB = evidence;

        // Parse files.
        Map<String, List<Map<String, String>>> dataByModel = new HashMap<>();
        for (File modelFolder : new File(folderWithRules + dataset).listFiles()) {
            String modelName = modelFolder.getName();

            // Skip tucker!
            if (modelName.equals("tucker"))
                continue;

            if (!dataByModel.containsKey(modelName))
                dataByModel.put(modelName, new ArrayList<>());

            // TODO Add files to parse here!
            for (String suffix : new String[]{"evaluated_inference_patterns_5_split_" + split_id, "evaluated_inference_patterns_asymmetry_5_split_" + split_id, "evaluated_inference_patterns_intersection_5_split_" + split_id}) {
                int lineNumber = 0;
                String[] header = null;
                // TODO Change split!
//					System.out.println(folderWithRules+dataset+"//" + modelName + "//"+dataset+"_"+modelName+"_"+suffix+".tsv");
                File f = new File(folderWithRules + dataset + "//" + modelName + "//" + dataset + "_" + modelName + "_" + suffix + ".tsv");
                if (!f.exists())
                    continue;
                Scanner sc = new Scanner(f);
                while (sc.hasNextLine()) {
                    String[] line = sc.nextLine().split("\t");
                    lineNumber++;

                    // Header!
                    if (lineNumber == 1)
                        header = line;
                    else {
                        Map<String, String> row = new HashMap<>();
                        for (int i = 0; i < header.length; i++)
                            row.put(header[i], line[i]);
                        dataByModel.get(modelName).add(row);
                    }
                }
                sc.close();
            }
        }
//			System.out.println(dataByModel);
        int bestA = -1, bestB = -1;
        double best = -1.0;
        Map<String, Map<String, List<Double>>> output = new HashMap<>();
        Map<String, AtomicInteger> totalRules = new HashMap<>();
        for (int aInt : new int[]{5}) {
            totalRules.clear();
            String a = "" + aInt;

            // Get pattern types, then models, then the desired numbers for a and b.
            Map<String, Map<String, List<Double>>> outputA = new HashMap<>(), outputB = new HashMap<>();
            for (String model : dataByModel.keySet()) {
                for (Map<String, String> row : dataByModel.get(model)) {
                    String patternType = row.get("Inference_pattern"), rule = row.get("Rule");
//                    System.out.println(row);
                    if (patternType.equals("Asymmetry"))
                        patternType = "Antisymmetry";

                    // Filters!
//                    System.out.println("|Support_D|: " + row.get("|Support_D|") + "; HC_dataset: " + row.get("HC_dataset") + "; PCA_dataset: " + row.get("PCA_dataset"));
                    if (/*Double.valueOf(row.get("|Support_D|")).longValue() < 10 || Double.parseDouble(row.get("HC_dataset")) < 0.1 || */Double.parseDouble(row.get("PCA_dataset")) < 0.1)
                        continue;

                    if (!row.get("K").equals(a))
                        continue;

                    // Some corrections to pattern types: transitive must be: x(?a, ?z), x(?z, ?b) ==> x(?a, ?b).
                    if (patternType.equals("Transitive") && (countDiffPreds(rule) != 1 || !isTransitive(rule, false))) {

                        patternType = correctType(rule);
//									System.out.println(patternType + " " + rule + " " + countDiffPreds(rule));
                    }
                    // composition must be: p_1(?a, ?z), p_2(?z, ?b) ==> p_3(?a, ?b) (transitive and predicates different).
                    if (patternType.equals("Composition") && (countDiffPreds(rule) != countAtoms(rule) || !isTransitive(rule, false)))
                        patternType = correctType(rule);

                    if (!outputA.containsKey(patternType)) {
                        outputA.put(patternType, new HashMap<>());
                        outputB.put(patternType, new HashMap<>());
                    }

                    if (!outputA.get(patternType).containsKey(model)) {
                        outputA.get(patternType).put(model, new ArrayList<>());
                        outputB.get(patternType).put(model, new ArrayList<>());
                    }

                    // TODO This is when we were doing things with different k's.
//								Map<String, Map<String, List<Double>>> outputToAdd = null;
//								if (row.get("K").equals(a))
//									outputToAdd = outputA;
//								else if (row.get("K").equals(b))
//									outputToAdd = outputB;
//								outputToAdd.get(patternType).get(model).add(Double.valueOf(row.get(toCollect)));

                    // TODO This is for the same k.
                    outputA.get(patternType).get(model).add(Double.valueOf(row.get(toCollectA)));
                    outputB.get(patternType).get(model).add(Double.valueOf(row.get(toCollectB)));

                    // TODO Extract some!
//								if (row.get("K").equals(a) && /*dataset.equals("WN18RR") && row.get("Rule").equals("3(?g,?b) 3(?a,?g) ==>3(?a,?b)")*/
//										patternType.equals("B.~Composition")) {
//									System.out.println("Dataset:" + dataset + "; Model: " + model + "; Rule: " + row.get("Rule") +
//											"; Value: " + Double.valueOf(row.get(toCollect)));
//								}
                }
            }

            for (String patternType : outputA.keySet()) {
                String modelKey = outputA.get(patternType).keySet().iterator().next();
                totalRules.put(patternType, new AtomicInteger(outputA.get(patternType).get(modelKey).size()));
            }
            output = outputA;
            DescriptiveStatistics overall = new DescriptiveStatistics(), diffs = new DescriptiveStatistics();
            DecimalFormat formatter = new DecimalFormat(".00");
            String[] patternsInOrder = new String[]{"Hierarchy", "Symmetry", "Antisymmetry", "Inversion", "Intersection", "Transitive", "Composition"};
            // TODO Models to print!
            String[] modelsInOrder = new String[]{"boxe", "complex", "hake", "hole", "quate", "rotate", "rotpro", "toruse", "transe"};
            String[] otherTypesInOrder = new String[]{"Gen.~Intersection", "B.~Transitive", "Equality", "B.~Composition", "Commonality",
                    "Any F.~Path", "Any B.~Path", "Any Commonality"};

            for (String patternType : patternsInOrder)
                printPatternType(patternType, modelsInOrder, formatter, expected, outputA, outputB, overall, diffs);
            System.out.println("--------------------------------------------------------------------");
            for (String patternType : otherTypesInOrder)
                printPatternType(patternType, modelsInOrder, formatter, expected, outputA, outputB, overall, diffs);

            if (overall.getMean() > best) {
                bestA = aInt;
                best = overall.getMean();
            }


            System.out.println("A: " + bestA + ";" + "Best: " + best);
            System.out.println("Total rules: " + totalRules);
            System.out.println();
            System.out.println();
            System.out.println();
            return output;
        }
        return null;
    }

    private static Table<String, String, String> getExpectedTable() {
        Table<String, String, String> expected = HashBasedTable.create();
        String currentPattern = "Hierarchy";
        expected.put(currentPattern, "boxe", YES_YES);
        expected.put(currentPattern, "complex", YES_NO);
        expected.put(currentPattern, "hake", UNK_UNK);
        expected.put(currentPattern, "hole", YES_NO);
        expected.put(currentPattern, "quate", UNK_UNK);
        expected.put(currentPattern, "rotate", NO_NO);
        expected.put(currentPattern, "rotpro", UNK_UNK);
        expected.put(currentPattern, "toruse", UNK_UNK);
        expected.put(currentPattern, "transe", NO_NO);

        currentPattern = "Symmetry";
        expected.put(currentPattern, "boxe", YES_YES);
        expected.put(currentPattern, "complex", YES_YES);
        expected.put(currentPattern, "hake", UNK_UNK);
        expected.put(currentPattern, "hole", UNK_UNK);
        expected.put(currentPattern, "quate", YES_UNK);
        expected.put(currentPattern, "rotate", YES_YES);
        expected.put(currentPattern, "rotpro", YES_UNK);
        expected.put(currentPattern, "toruse", UNK_UNK);
        expected.put(currentPattern, "transe", NO_NO);

        currentPattern = "Antisymmetry";
        expected.put(currentPattern, "boxe", YES_YES);
        expected.put(currentPattern, "complex", YES_YES);
        expected.put(currentPattern, "hake", UNK_UNK);
        expected.put(currentPattern, "hole", UNK_UNK);
        expected.put(currentPattern, "quate", YES_UNK);
        expected.put(currentPattern, "rotate", YES_YES);
        expected.put(currentPattern, "rotpro", YES_UNK);
        expected.put(currentPattern, "toruse", UNK_UNK);
        expected.put(currentPattern, "transe", YES_YES);

        currentPattern = "Inversion";
        expected.put(currentPattern, "boxe", YES_YES);
        expected.put(currentPattern, "complex", YES_YES);
        expected.put(currentPattern, "hake", UNK_UNK);
        expected.put(currentPattern, "hole", UNK_UNK);
        expected.put(currentPattern, "quate", YES_UNK);
        expected.put(currentPattern, "rotate", YES_YES);
        expected.put(currentPattern, "rotpro", YES_UNK);
        expected.put(currentPattern, "toruse", UNK_UNK);
        expected.put(currentPattern, "transe", YES_NO);

        currentPattern = "Intersection";
        expected.put(currentPattern, "boxe", YES_YES);
        expected.put(currentPattern, "complex", NO_NO);
        expected.put(currentPattern, "hake", UNK_UNK);
        expected.put(currentPattern, "hole", UNK_UNK);
        expected.put(currentPattern, "quate", UNK_UNK);
        expected.put(currentPattern, "rotate", YES_NO);
        expected.put(currentPattern, "rotpro", UNK_UNK);
        expected.put(currentPattern, "toruse", UNK_UNK);
        expected.put(currentPattern, "transe", YES_NO);

        currentPattern = "Transitive";
        expected.put(currentPattern, "boxe", UNK_UNK);
        expected.put(currentPattern, "complex", NO_UNK);
        expected.put(currentPattern, "hake", UNK_UNK);
        expected.put(currentPattern, "hole", UNK_UNK);
        expected.put(currentPattern, "quate", NO_UNK);
        expected.put(currentPattern, "rotate", NO_UNK);
        expected.put(currentPattern, "rotpro", YES_UNK);
        expected.put(currentPattern, "toruse", UNK_UNK);
        expected.put(currentPattern, "transe", NO_UNK);

        currentPattern = "Composition";
        expected.put(currentPattern, "boxe", NO_NO);
        expected.put(currentPattern, "complex", NO_NO);
        expected.put(currentPattern, "hake", UNK_UNK);
        expected.put(currentPattern, "hole", UNK_UNK);
        expected.put(currentPattern, "quate", UNK_UNK);
        expected.put(currentPattern, "rotate", YES_NO);
        expected.put(currentPattern, "rotpro", YES_UNK);
        expected.put(currentPattern, "toruse", UNK_UNK);
        expected.put(currentPattern, "transe", YES_NO);
        return expected;
    }


    private static void printPatternType(String patternType, String[] modelsInOrder, DecimalFormat formatter, Table<String, String, String> expected,
                                         Map<String, Map<String, List<Double>>> outputA, Map<String, Map<String, List<Double>>> outputB,
                                         DescriptiveStatistics overall, DescriptiveStatistics diffs) {
        if (!outputA.containsKey(patternType)) {
            System.out.println("% & " + patternType + " &\\multicolumn{9}{c|}{--}\\\\ \\cline{2-11}");
            return;
        }

        System.out.print("& " + patternType);
        for (String model : modelsInOrder)
            if (outputA.get(patternType).containsKey(model)) {
                DescriptiveStatistics aStats = new DescriptiveStatistics(), bStats = new DescriptiveStatistics(), aMinusBStats = new DescriptiveStatistics();

                for (double value : outputA.get(patternType).get(model))
                    aStats.addValue(value);
                for (double value : outputB.get(patternType).get(model))
                    bStats.addValue(value);

                // We assume the same rules appear in the same positions!
                for (int i = 0; i < outputA.get(patternType).get(model).size(); i++)
                    aMinusBStats.addValue(outputA.get(patternType).get(model).get(i) - outputB.get(patternType).get(model).get(i));

                overall.addValue(aStats.getMean());
                diffs.addValue(aMinusBStats.getMean());
                // TODO What do you want to print?
                System.out.print(" & " + formatter.format(aStats.getMean())
                        //					" \\quad $"+(expected.contains(patternType, model)?expected.get(patternType, model):UNK_UNK)+"$ " +
//                        " (" + formatter.format(bStats.getMean()) + ")" +
//					" (" + formatter.format(aMinusBStats.getMean()) + ")" +
//                        ""
                );
            } else
                System.out.print(" & --");

        System.out.print("\\\\ \\cline{2-11} \n");
    }

    // This counts atoms in the input rule (e.g., "17(?g,?b) 17(?a,?g) ==>17(?a,?b)"), including head.
    private static int countAtoms(String rule) {
        String[] bodyAndHead = rule.split(" ==>");
        String body = bodyAndHead[0];
        return body.split(" ").length + 1;
    }

    // This computes different predicates in the input rule (e.g., "17(?g,?b) 17(?a,?g) ==>17(?a,?b)").
    private static int countDiffPreds(String rule) {
        String[] bodyAndHead = rule.split(" ==> ");
        String head = bodyAndHead[1];

        Set<String> preds = getBodyPredicates(rule);
        preds.add(getPredicate(head));

        return preds.size();
    }

    private static Set<String> getBodyPredicates(String rule) {
        String[] bodyAndHead = rule.split(" ==> ");
        String body = bodyAndHead[0];
        Set<String> preds = new HashSet<>();
        for (String bAtom : body.split(" "))
            preds.add(getPredicate(bAtom));
        return preds;
    }

    private static Set<String> getVars(String rule) {
        String[] bodyAndHead = rule.split(" ==> ");
        String body = bodyAndHead[0], head = bodyAndHead[1];

        Set<String> vars = new HashSet<>();

        vars.add(getSubject(head));
        vars.add(getObject(head));

        for (String bAtom : body.split(" ")) {
            vars.add(getSubject(bAtom));
            vars.add(getObject(bAtom));
        }

        return vars;
    }

    private static String getPredicate(String atom) {
        return atom.split("\\(")[0];
    }

    private static String getSubject(String atom) {
        return atom.split("\\(")[1].split(",")[0];
    }

    private static String getObject(String atom) {
        return atom.split("\\(")[1].split(",")[1].replace(")", "");
    }

    // This computes whether the input rule (e.g., "17(?g,?b) 17(?a,?g) ==>17(?a,?b)") is transitive.
    private static boolean isTransitive(String rule, boolean inverse) {
        String[] bodyAndHead = rule.split(" ==> ");
        String head = bodyAndHead[1], body = bodyAndHead[0];

//		System.out.println("Head: " + head + "; Body: " + body);
        int bodySize = 0;
        Graph<String, DefaultEdge> g = new DirectedPseudograph<>(DefaultEdge.class);
        for (String bAtom : body.split(" ")) {
            String s = getSubject(bAtom), o = getObject(bAtom);
//			System.out.println(bAtom + " Adding edge " + s + " -> " + o);

            if (!g.vertexSet().contains(s))
                g.addVertex(s);
            if (!g.vertexSet().contains(o))
                g.addVertex(o);

            g.addEdge(s, o);
            bodySize++;
        }

        // Find path from s to o.
        String s = getSubject(head), o = getObject(head);

        DijkstraShortestPath<String, DefaultEdge> dijkstra = new DijkstraShortestPath<>(g);

        String init = s, end = o;
        if (inverse) {
            init = o;
            end = s;
        }

        GraphPath<String, DefaultEdge> path = dijkstra.getPath(init, end);

        return path != null && path.getLength() == bodySize;
    }

    // This takes a rule and returns the new type of rule. We distinguish between inverse path/any subgraph, and same predicates/same body/one body and head/all different predicates.
    private static String correctType(String rule) {
        StringBuffer newType = new StringBuffer();

        if (isTransitive(rule, false))
            newType.append("Transitive");
        else if (isTransitive(rule, true))
            newType.append("InverseTransitive");
        else
            newType.append("Subgraph");

        int ruleSize = countAtoms(rule), diffPreds = countDiffPreds(rule);

//		String[] bodyAndHead = rule.split(" ==>");
//		String headPred = getPredicate(bodyAndHead[1]);

        if (diffPreds == 1)
            newType.append("SamePreds");
        else if (diffPreds == ruleSize)
            newType.append("DiffPreds");
            // We do not care for the rest.
        else
            newType.append("*");
//		else if (getBodyPredicates(rule).size() == 1)
//			newType.append("SameBody");
//		else if (getBodyPredicates(rule).contains(headPred))
//			newType.append("HeadInBody");
//		else
//			newType.append("Other!!!!!!!");

        if (getVars(rule).size() == 2) {
            newType.setLength(0);
            newType.append("Gen.~Intersection");
        }

        // Print something we can put on a paper!
        return
                switch (newType.toString()) {
                    case "Gen.~Intersection":
                        yield "Gen.~Intersection";
                    case "InverseTransitiveSamePreds":
                        yield "B.~Transitive";
                    case "InverseTransitiveDiffPreds":
                        yield "B.~Composition";
                    case "SubgraphSamePreds":
                        yield "Equality";
                    case "SubgraphDiffPreds":
                        yield "Commonality";
                    case "Subgraph*":
                        yield "Any Commonality";
                    case "Transitive*":
                        yield "Any F.~Path";
                    case "InverseTransitive*":
                        yield "Any B.~Path";
                    default:
                        throw new IllegalArgumentException("Unexpected value: " + newType.toString());
                };
    }

}
