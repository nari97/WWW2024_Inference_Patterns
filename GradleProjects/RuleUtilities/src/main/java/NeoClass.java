import com.google.common.io.MoreFiles;
import com.google.common.io.RecursiveDeleteOption;
import org.neo4j.batchinsert.BatchInserter;
import org.neo4j.batchinsert.BatchInserters;
import org.neo4j.configuration.Config;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.dbms.api.DatabaseManagementServiceBuilder;
import org.neo4j.graphdb.*;
import org.neo4j.io.layout.DatabaseLayout;

import java.io.File;
import java.io.IOException;
import java.util.*;

public class NeoClass {
    String database_folder_path = null;
    String memory_allocation = "128G";

    public NeoClass(String database_folder_path){
        this.database_folder_path = database_folder_path;
    }

    public NeoClass(String database_folder_path, String memory_allocation){
        this.database_folder_path = database_folder_path;
        this.memory_allocation = memory_allocation;
    }

    public void create_neo4j_database(ArrayList<ArrayList<Integer>> triples_to_add) throws IOException {
        /**
         Creates a neo4j database based on the given triples.
         @param database_folder_path the path of the folder where the neo4j database will be stored
         @param triples_to_add an ArrayList of ArrayList of integers, where each inner ArrayList represents a triple in the form (subject, predicate, object)
         @throws IOException if an I/O error occurs while accessing the database folder
         */

        File neo4j_folder = new File(this.database_folder_path);
        if (neo4j_folder.exists())
            MoreFiles.deleteRecursively(neo4j_folder.toPath(), RecursiveDeleteOption.ALLOW_INSECURE);
        BatchInserter inserter = BatchInserters.inserter(DatabaseLayout.of(
                Config.newBuilder().set(GraphDatabaseSettings.neo4j_home, neo4j_folder.toPath()).build()));

        for (ArrayList<Integer> triple: triples_to_add){
            int s = triple.get(0);
            int p = triple.get(1);
            int o = triple.get(2);

            if (!inserter.nodeExists(s))
                inserter.createNode(s, new HashMap<>(), Label.label("Node"));
            if (!inserter.nodeExists(o))
                inserter.createNode(o, new HashMap<>(), Label.label("Node"));

            inserter.createRelationship(s, o, RelationshipType.withName("" + p), new HashMap<>());
        }

        inserter.shutdown();
    }

    public String construct_pca_query(Rule rule_to_query, String nonFuncVar){
        /**
         Constructs a query for the given rule.
         @param rule_to_query the rule to query
         @param nonFuncVar the non-functional variable of the rule
         @return the query
         */

        StringBuilder query = new StringBuilder();
        for(Atom atom: rule_to_query.body_atoms)
            query.append(" MATCH ").append(atom.neo4j_print());

        query.append(" MATCH ").append(rule_to_query.head_atom.neo4j_print().replace(nonFuncVar, "z"));
        query.append(" WHERE ").append(nonFuncVar).append("<>").append("z");
        query.append(" RETURN id(a) as a, id(b) as b");

        return query.toString();
    }

    public  String construct_support_query(Rule rule_to_query){
        /**
         Constructs a query for the given rule.
         @param rule_to_query the rule to query
         @return the query
         */

        StringBuilder query = new StringBuilder();
        for(Atom atom: rule_to_query.body_atoms)
            query.append(" MATCH ").append(atom.neo4j_print());

        query.append(" MATCH ").append(rule_to_query.head_atom.neo4j_print());
        query.append(" RETURN id(a) as a, id(b) as b");

        return query.toString();
    }

    public List<Object> query_rule_asymmetry_memory_efficient(Rule rule_to_query){

        DatabaseManagementService service = this.get_service(this.database_folder_path);

        GraphDatabaseService db = service.database("neo4j");
        Transaction tx = db.beginTx();
        long support = 0;
        String non_functional_variable = rule_to_query.functional_variable.equals("?b")? "a" : "b";
//        System.out.println("Functional variable: " + rule_to_query.functional_variable);
//        System.out.println("Non-functional variable: " + non_functional_variable);
//        System.out.println("Support query: " + "MATCH " + rule_to_query.body_atoms.get(0).neo4j_print() + " WHERE NOT EXISTS(" + rule_to_query.head_atom.neo4j_print() + ") WITH DISTINCT a, b RETURN id(a) as a, id(b) as b");
//        System.out.println("PCA query: " + "MATCH " + rule_to_query.body_atoms.get(0).neo4j_print() + " WHERE NOT EXISTS(" + rule_to_query.head_atom.neo4j_print().replace(non_functional_variable, "") + ") WITH DISTINCT a, b RETURN id(a) as a, id(b) as b");
        Set<String> support_set = new HashSet<>();
        Set<String> pca_set = new HashSet<>();
        try {
            Result res = tx.execute("MATCH " + rule_to_query.body_atoms.get(0).neo4j_print() + " WHERE NOT EXISTS(" + rule_to_query.head_atom.neo4j_print() + ") WITH DISTINCT a, b RETURN id(a) as a, id(b) as b");
            while (res.hasNext()) {
                Map<String, Object> row = res.next();
                long a = (long) row.get("a");
                long b = (long) row.get("b");
                support_set.add(a + "," + b);

            }
            res.close();
        } catch (Exception e) {
            e.printStackTrace();
        }

        try {
            Result res = tx.execute("MATCH " + rule_to_query.body_atoms.get(0).neo4j_print() + " WHERE NOT EXISTS(" + rule_to_query.head_atom.neo4j_print().replace(non_functional_variable, "") + ") WITH DISTINCT a, b RETURN id(a) as a, id(b) as b");
            while (res.hasNext()) {
                Map<String, Object> row = res.next();
                long a = (long) row.get("a");
                long b = (long) row.get("b");
                pca_set.add(a + "," + b);
            }
            res.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
        long tHeads = 0;
        try {
            Result res = tx.execute("MATCH " +  rule_to_query.head_atom.neo4j_print() + " WITH COLLECT(DISTINCT a) as subjects, COLLECT(DISTINCT b) AS objects RETURN SIZE(subjects)*SIZE(objects) as denom");
            while (res.hasNext()) {
                Map<String, Object> row = res.next();
                long n = (long) row.get("denom");
                tHeads = n;
            }
            res.close();
        } catch (Exception e) {
            e.printStackTrace();
        }

        try {
            Result res = tx.execute("MATCH " +  rule_to_query.head_atom.neo4j_print() + " RETURN COUNT(*) as cnt_relations");
            while (res.hasNext()) {
                Map<String, Object> row = res.next();
                long n = (long) row.get("cnt_relations");
                tHeads -= n;
            }
            res.close();
        } catch (Exception e) {
            e.printStackTrace();
        }

        pca_set.addAll(support_set);
        System.out.println("\tComputed support: " + support_set.size());
        System.out.println("\tComputed PCA: " + pca_set.size());
        System.out.println("\tHead size: " + tHeads);

        double hc_for_rule = tHeads> 0?(1.0*support_set.size())/tHeads : 0.0;
        double pca_for_rule = tHeads>0?(1.0*support_set.size())/pca_set.size() : 0.0;

        ArrayList<Object> result = new ArrayList<>();
        result.add(support_set);
        result.add(pca_set);
        result.add(hc_for_rule);
        result.add(pca_for_rule);
        tx.close();

        service.shutdown();

        return result;
    }

    public List<Object> query_rule_memory_efficient(Rule rule_to_query){
        DatabaseManagementService service = this.get_service(this.database_folder_path);

        GraphDatabaseService db = service.database("neo4j");
        String nonFuncVar = rule_to_query.functional_variable.equals("a")?"b":"a";

        //Build query
        StringBuilder query = new StringBuilder();
        Result res = null;

        for(Atom atom: rule_to_query.body_atoms)
            query.append(" MATCH ").append(atom.neo4j_print());

        Transaction tx = db.beginTx();

        System.out.println("\t -- Running body query");

        //Define hashmap
        Map<Long, Set<Long>> bodyPairs = new HashMap<>();
        int body_size = 0;
        try {
            res = tx.execute(query + " RETURN id(a) AS a, id(b) AS b");
            while (res.hasNext()) {
                Map<String, Object> row = res.next();
                // Get functional and non functional variable
                long fv = (long) row.get(rule_to_query.functional_variable), nfv = (long) row.get(nonFuncVar);

                // If nv does not exist, add nv using fv as key
                if (!bodyPairs.containsKey(fv))
                    bodyPairs.put(fv, new HashSet<>());

                bodyPairs.get(fv).add(nfv);
            }
            res.close();
        } catch(Exception e){
            e.printStackTrace();
        }

        for(long fv: bodyPairs.keySet()){
            body_size += bodyPairs.get(fv).size();
        }

        System.out.println("\t -- Running head query");

        //Define hash set for support and pca
        Set<String> support = new HashSet<>(), pca = new HashSet<>();
        int totalHeads = 0;

        // Define set for functional variables
        Set<Long> allFVs = new HashSet<>();

        // Define query for head atom
        res = tx.execute("MATCH " + rule_to_query.head_atom.neo4j_print() + " RETURN id(a) AS a, id(b) AS b");
        while (res.hasNext()) {
            Map<String, Object> row = res.next();

            //Get fv and nfv
            long fv = (long) row.get(rule_to_query.functional_variable), nfv = (long) row.get(nonFuncVar);

            // Add fv to fv set
            allFVs.add(fv);

            // If bodypairs contains fv and the value is the nfv, add to support
            if (bodyPairs.containsKey(fv) && bodyPairs.get(fv).contains(nfv))
                support.add(fv + "," + nfv);

            totalHeads++;
        }
        res.close();

        System.out.println("\t -- Computing PCA");

        // Compute pca
        // Iterate through all fvs
        for (Long fv : allFVs)
            // If bodyPairs contains the fv
            if (bodyPairs.containsKey(fv))
                // For all values in key of body pairs, add to pca
                for (Long other : bodyPairs.get(fv))
                    pca.add(fv + "," + other);

        System.out.println("\tBody pairs size: " + bodyPairs.size());
        System.out.println("\tCalculated body pairs size: " + body_size);
        System.out.println("\tHead size: " + totalHeads);
        System.out.println("\tSupport size: " + support.size());
        System.out.println("\tPCA size: " + pca.size());
        double hc_for_rule = (1.0*support.size())/totalHeads;
        double pca_for_rule = (1.0*support.size())/pca.size();



        ArrayList<Object> result = new ArrayList<>();
        result.add(support);
        result.add(pca);
        result.add(hc_for_rule);
        result.add(pca_for_rule);
        service.shutdown();

        return result;
    }

    public DatabaseManagementService get_service(String database_folder_path){
        /**
         Returns a DatabaseManagementService for the given database folder.
         @param database_folder_path the path of the folder where the neo4j database is stored
         @return the DatabaseManagementService
         */

        File neo4j_folder = new File(database_folder_path);

        return new DatabaseManagementServiceBuilder(neo4j_folder.toPath()).
                setConfig(GraphDatabaseSettings.keep_logical_logs, "false").
                setConfig(GraphDatabaseSettings.preallocate_logical_logs, false).
                setConfig(GraphDatabaseSettings.pagecache_memory, this.memory_allocation).
                setConfig(GraphDatabaseSettings.keep_logical_logs, "false").
                setConfig(GraphDatabaseSettings.preallocate_logical_logs, false).build();
    }

    public Set<String> get_positive_instantiations(Rule rule_to_query){
        Set<String> positives = new HashSet<>();
        DatabaseManagementService service = this.get_service(this.database_folder_path);

        GraphDatabaseService db = service.database("neo4j");
        Transaction tx = db.beginTx();

        String support_query = this.construct_support_query(rule_to_query);
        try {
            Result res = tx.execute(support_query);
            while (res.hasNext()) {
                Map<String, Object> row = res.next();
                long a = (long) row.get("a"), b = (long) row.get("b");
                positives.add(a + "," + b);
            }
            res.close();
        } catch(Exception e){
            e.printStackTrace();
        }
        service.shutdown();

        return positives;
    }

    public HashMap<String, List<Integer>> get_negative_instantiations_both_functional(Rule rule_to_query){

        DatabaseManagementService service = this.get_service(this.database_folder_path);
        GraphDatabaseService db = service.database("neo4j");
        Transaction tx = db.beginTx();

        String subject_query = this.construct_pca_query(rule_to_query, "a");
        Set<String> subject_pca = new HashSet<>();

        try {
            Result res = tx.execute(subject_query);
            while (res.hasNext()) {
                Map<String, Object> row = res.next();
                long a = (long) row.get("a"), b = (long) row.get("b");
                subject_pca.add(a + "," + b);
            }
            res.close();
        } catch(Exception e){
            e.printStackTrace();
        }

        String object_query = this.construct_pca_query(rule_to_query, "b");
        Set<String> object_pca = new HashSet<>();
        try{
            Result res = tx.execute(object_query);
            while (res.hasNext()) {
                Map<String, Object> row = res.next();
                long a = (long) row.get("a"), b = (long) row.get("b");
                object_pca.add(a + "," + b);
            }
            res.close();
        } catch(Exception e){
            e.printStackTrace();
        }
        service.shutdown();

        System.out.println("Number of instantiations for nonFuncVar a: " + subject_pca.size());
        System.out.println("Number of instantiations for nonFuncVar b: " + object_pca.size());
        HashMap<String, List<Integer>> result = new HashMap<>();

        for(String inst: subject_pca){
            if(!result.containsKey(inst)) {
                List<Integer> temp = new ArrayList<>();
                temp.add(1);
                temp.add(0);
                result.put(inst, temp);
            }
        }

        for(String inst: object_pca){
            List<Integer> temp;
            if(!result.containsKey(inst)){
                temp = new ArrayList<>();
                temp.add(0);
                temp.add(1);
            }
            else{
                temp = result.get(inst);
                temp.set(1, 1);
            }
            result.put(inst, temp);
        }

//        System.out.println("Total number of negatives: " + result.size());
        return result;
    }
}
