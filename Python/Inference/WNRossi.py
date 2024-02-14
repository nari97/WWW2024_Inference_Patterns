from CompareWN import get_inference_pattern_overlap_metrics, get_best_rules, load_datasets, get_interested_rules


def report_test_facts_percentage(path_to_data, dataset, pattern, pattern_name):
    path_to_dataset_folder = f"{path_to_data}/Datasets/{dataset}"
    test_facts_ctr = 0
    pattern_ctr = 0
    with open(f"{path_to_dataset_folder}/test2id.txt", "r") as f:
        f.readline()
        for line in f:
            splits = line.strip().split("\t")
            if len(splits) == 1:
                splits = line.strip().split(" ")
            test_facts_ctr += 1
            if splits[2] in pattern:
                pattern_ctr += 1

    print(
        f"Pattern: {pattern_name}; Test facts: {test_facts_ctr}; Pattern facts: {pattern_ctr}; Percentage: {round(pattern_ctr * 100.0 / test_facts_ctr, 2)}%")


def get_triples(path_to_data, dataset):
    path_to_dataset_folder = f"{path_to_data}/Datasets/{dataset}"
    triples = {}
    triple_count = 0
    for split in ["train2id.txt"]:
        with open(f"{path_to_dataset_folder}/{split}", "r") as f:
            f.readline()
            for line in f:
                triple_count += 1
                splits = line.strip().split("\t")
                if len(splits) == 1:
                    splits = line.strip().split(" ")
                if splits[2] not in triples:
                    triples[splits[2]] = {}

                if splits[0] not in triples[splits[2]]:
                    triples[splits[2]][splits[0]] = []

                triples[splits[2]][splits[0]].append(splits[1])
    print(f"Total triples for dataset {dataset}: {triple_count}")
    return triples


def get_symmetric_relations(triples):
    symmetric_relations = {}
    for relation in triples:
        ctr = 0
        triple_cnt = 0
        for head in triples[relation]:
            for tail in triples[relation][head]:
                if head == tail:
                    continue
                triple_cnt += 1
                if tail in triples[relation] and head in triples[relation][tail]:
                    ctr += 1

        symmetric_relations[relation] = ctr * 1.0 / triple_cnt

    return symmetric_relations


def get_asymmetric_relations(triples):
    asymmetric_relations = {}
    for relation in triples:
        ctr = 0
        triple_cnt = 0
        for head in triples[relation]:
            for tail in triples[relation][head]:
                if head == tail:
                    continue
                triple_cnt += 1
                if tail not in triples[relation] or head not in triples[relation][tail]:
                    ctr += 1

        asymmetric_relations[relation] = ctr * 1.0 / triple_cnt

    return asymmetric_relations


def get_transitive_relations(triples):
    transitive_relations = {}

    for relation in triples:
        ctr = 0
        triple_cnt = 0
        for head in triples[relation]:
            for tail in triples[relation][head]:
                if head == tail:
                    continue
                triple_cnt += 1
                flag = False
                for int_tail in triples[relation][head]:
                    if int_tail == tail or int_tail == head:
                        continue
                    if int_tail in triples[relation] and tail in triples[relation][int_tail]:
                        flag = True
                        break

                if flag:
                    ctr += 1
        transitive_relations[relation] = ctr * 1.0 / triple_cnt

    return transitive_relations


def compute_pattern_allignment_metrics(rules, best_types_for_predicates, k):
    model_stats = {}
    for model in rules:
        model_stats[model] = {}
        for relation in best_types_for_predicates:
            best_score = -0.01
            best_type = ""
            for rule in rules[model]:
                # print(rule["Rule"].id_print())
                head_relation = rule["Rule"].head_atom.relationship
                if rule["Pattern"] == "Transitive":
                    r = rule["Rule"]
                    if r.body_atoms[0].variable2 != r.body_atoms[1].variable1:
                        continue
                if head_relation == int(relation) and rule["K"] == k and rule["Pattern"] in ["Symmetry", "Transitive", "Asymmetry"]:

                    if float(rule["Support_DS_filtered"]) > best_score:
                        best_score = float(rule["Support_DS_filtered"])
                        best_type = rule["Pattern"]

            model_stats[model][relation] = [best_type, best_score]
    print("\n")
    for model in model_stats:
        ctr = 0
        errors = {}
        relation_count = len(best_types_for_predicates)
        for relation in best_types_for_predicates:
            if model_stats[model][relation][0] == best_types_for_predicates[relation][0]:
                ctr += 1
            else:
                errors[relation] = {"Expected": best_types_for_predicates[relation][0], "Got": model_stats[model][relation][0]}

        print(f"Model: {model}; Accuracy: {round(ctr * 1.0 / relation_count, 2)}%")
        print(f"Errors: {errors}")
        print("\n")
    # return model_stats


if __name__ == "__main__":
    path_to_data = r"D:\PhD\Work\Phd_research\InferenceMinedRules"
    models = ["boxe", "complex", "hake", "hole", "quate", "rotate", "rotpro", "toruse", "transe"]
    k = "5"
    cutoff = 0.5
    dataset = "YAGO3-10"
    triples = get_triples(path_to_data, dataset)
    symmetric = get_symmetric_relations(triples)
    print("Relations that are symmetric (>0.5): ")
    result = []
    for relation in symmetric:
        if symmetric[relation] >= cutoff:
            result.append(relation)
    print(result)
    # print("Ratio: ", len(result) / len(symmetric))
    report_test_facts_percentage(path_to_data, dataset, result, "Symmetry")
    print("\n")

    asymmetric = get_asymmetric_relations(triples)
    print("Relations that are asymmetric (>0.5): ")
    result = []
    for relation in asymmetric:
        if asymmetric[relation] >= 0.5:
            result.append(relation)
    print(result)
    # print("Ratio: ", len(result) / len(asymmetric))
    report_test_facts_percentage(path_to_data, dataset, result, "Asymmetry")
    print("\n")
    # print("Asymmetric: ", asymmetric)

    transitive = get_transitive_relations(triples)
    print("Relations that are transitive (>0.5): ")
    result = []
    for relation in transitive:
        if transitive[relation] >= cutoff:
            result.append(relation)
    print(result)
    # print("Ratio: ", len(result) / len(transitive))
    report_test_facts_percentage(path_to_data, dataset,result, "Transitivity")
    best_types_for_predicates = {}
    for relation in symmetric:
        best_type = ""
        best_score = 0.0
        if symmetric[relation] >= best_score:
            best_score = symmetric[relation]
            best_type = "Symmetry"
        if asymmetric[relation] >= best_score:
            best_score = asymmetric[relation]
            best_type = "Asymmetry"
        if transitive[relation] >= best_score:
            best_score = transitive[relation]
            best_type = "Transitive"

        best_types_for_predicates[relation] = [best_type, best_score]

    rules_all = get_inference_pattern_overlap_metrics(path_to_data, dataset, models)
    rules_asymmetric = get_inference_pattern_overlap_metrics(path_to_data, dataset, models, "A")
    # for rule in rules_asymmetric["boxe"]:
    #     print(rule["Rule"].id_print(), rule["Support_DS_filtered"])
    #Combine both
    rules = {}
    for model in rules_all:
        rules[model] = rules_all[model] + rules_asymmetric[model]

    print("\n")
    # print(len(best_types_for_predicates))
    # print(best_types_for_predicates)
    compute_pattern_allignment_metrics(rules, best_types_for_predicates, k)
    # print(model_stats)