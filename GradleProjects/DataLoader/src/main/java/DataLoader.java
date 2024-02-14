import java.io.*;
import java.util.*;

class TripleLoader {
    HashMap<Integer, ArrayList<ArrayList<Integer>>> triple_dict = null;
    public TripleLoader(String path_to_triples, boolean ignore_first_line, boolean is_relation_second) throws FileNotFoundException {
        this.triple_dict = load_triples(path_to_triples, ignore_first_line, is_relation_second);
    }

    public HashMap<Integer, ArrayList<ArrayList<Integer>>> load_triples(String path_to_triples, boolean ignore_first_line, boolean is_relation_second) throws FileNotFoundException {
        HashMap<Integer, ArrayList<ArrayList<Integer>>> triple_dict = new HashMap<>();
        Scanner sc = new Scanner(new File(path_to_triples));

        if (ignore_first_line) {
            sc.nextLine();
        }

        while (sc.hasNext()) {

            String line = sc.nextLine();
            if (line.equals("\n") || line.equals(""))
                continue;
            String[] splits = line.strip().split("\t");
            if (splits.length == 1) {
                splits = line.strip().split(" ");
            }
            int s = 0;
            int p = 0;
            int o = 0;
            try {

                s = Integer.parseInt(splits[0]);
                if (is_relation_second){
                    p = Integer.parseInt(splits[1]);
                    o = Integer.parseInt(splits[2]);
                } else {
                    p = Integer.parseInt(splits[2]);
                    o = Integer.parseInt(splits[1]);
                }
            } catch (NumberFormatException e) {
                //Exception because input string was a float, so we need to use Double.parseDouble and then convert
                //to int
                s = (int) Double.parseDouble(splits[0]);
                if (is_relation_second){
                    p = (int) Double.parseDouble(splits[1]);
                    o = (int) Double.parseDouble(splits[2]);
                } else {
                    p = (int) Double.parseDouble(splits[2]);
                    o = (int) Double.parseDouble(splits[1]);
                }
//                    System.out.println("Caught a double and converted to int\t" + splits[0] + "\t" + splits[1] + "\t"
//                            + splits[2] + "\t" + s + "\t" + p + "\t" + o);
            }


            if (!triple_dict.containsKey(p)) {
                ArrayList<ArrayList<Integer>> temp = new ArrayList<>();
                triple_dict.put(p, temp);
            }

            ArrayList<Integer> triple = new ArrayList<>();
            triple.add(s);
            triple.add(p);
            triple.add(o);
            triple_dict.get(p).add(triple);
        }

        sc.close();

        return triple_dict;
    }

    public int get_triple_count() {
        int triple_count = 0;
        for (int key : this.triple_dict.keySet()) {
            triple_count += this.triple_dict.get(key).size();
        }
        return triple_count;
    }
}