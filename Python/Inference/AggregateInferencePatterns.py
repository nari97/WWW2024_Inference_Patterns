import sys
import pandas as pd
import math
from ParseRules import Atom, Rule

pd.set_option('display.max_columns', None)


def extract_rule(rule_str):
    input_list = rule_str.split(' ')
    output_list = []
    # print(rule_str)
    for item in input_list:
        if '==>' in item:
            item = item.replace('==>', '')
        if '=>' in item:
            continue

        num, variable1, variable2 = item.replace(')', '').replace('(', '').split('?')
        output_list.append([int(num), variable1.replace(",", ''), variable2.replace(',', '')])

    return output_list


def get_best_avg_rules(rules_by_pattern, k, dataset, id_to_relation):
    print("Dataset: ", dataset)
    result = []
    for pattern in rules_by_pattern:
        if pattern in ["Asymmetry", "Intersection"]:
            continue
        n_df = rules_by_pattern[pattern]
        # print(n_df)
        n_df = n_df[n_df["K"] == str(k)]

        rules = n_df["Rule"].unique()
        for rule in rules:
            inner_result = []
            rule_df = n_df[n_df["Rule"] == rule]
            inner_result.append(rule)
            inner_result.append(pattern)
            inner_result.append(rule_df["Support_DS_filtered"].mean())
            for model in ["boxe", "complex", "hake", "hole", "quate", "rotate", "rotpro", "toruse", "transe"]:
                model_df = rule_df[rule_df["Model"] == model]
                inner_result.append(model_df["Support_DS_filtered"].values[0])

            result.append(inner_result)

    all_rules_df = pd.DataFrame(result,
                                columns=["Rule", "Pattern", "Avg", "boxe", "complex", "hake", "hole", "quate", "rotate",
                                         "rotpro", "toruse", "transe"])

    min_avg_row = all_rules_df.loc[all_rules_df["Avg"].idxmin()]

    # Find the row with the highest Avg value
    max_avg_row = all_rules_df.loc[all_rules_df["Avg"].idxmax()]

    min_best_value = -0.1
    min_best_column = ""
    min_worst_value = 1.01
    min_worst_column = ""

    max_best_value = -0.1
    max_best_column = ""
    max_worst_value = 1.01
    max_worst_column = ""

    # Iterate through the columns starting from the 4th column
    for column in min_avg_row.index[4:]:
        # Check for min_avg_row
        if min_avg_row[column] > min_best_value:
            min_best_value = round(min_avg_row[column],2)
            min_best_column = column

        if min_avg_row[column] < min_worst_value:
            min_worst_value = round(min_avg_row[column],2)
            min_worst_column = column

        # Check for max_avg_row
        if max_avg_row[column] > max_best_value:
            max_best_value = round(max_avg_row[column],2)
            max_best_column = column

        if max_avg_row[column] < max_worst_value:
            max_worst_value = round(max_avg_row[column],2)
            max_worst_column = column

    print("Worst rule:", min_avg_row["Rule"].id_print(), min_avg_row["Pattern"], min_worst_column, min_worst_value, min_best_column, min_best_value)
    print("Best rule:", max_avg_row["Rule"].id_print(), max_avg_row["Pattern"], max_worst_column, max_worst_value, max_best_column, max_best_value)
    print()


def get_best_rules(rules_by_pattern, k, dataset, id_to_relation):
    print("Dataset: ", dataset)
    for pattern in rules_by_pattern:

        n_df = rules_by_pattern[pattern]
        # print(n_df)
        n_df = n_df[n_df["K"] == str(k)]

        rules = n_df["Rule"].unique()

        results = []
        for rule in rules:
            minModel = ""
            minModelValue = 1.01
            maxModel = ""
            maxModelValue = 0.0
            x_df = n_df[n_df["Rule"] == rule]
            # print(rule.id_print(), x_df)
            for index, row in x_df.iterrows():

                if float(row["Support_DS_filtered"]) <= minModelValue:
                    minModelValue = float(row["Support_DS_filtered"])
                    minModel = row["Model"]

                if float(row["Support_DS_filtered"]) >= maxModelValue:
                    maxModelValue = float(row["Support_DS_filtered"])
                    maxModel = row["Model"]

            results.append([rule, minModel, minModelValue, maxModel, maxModelValue])
        df = pd.DataFrame(results, columns=["Rule", "MinModel", "MinModelValue", "MaxModel", "MaxModelValue"])

        df["Difference"] = df["MaxModelValue"] - df["MinModelValue"]
        df = df.sort_values(by=["Difference"], ascending=False).head(2)
        print("\tPattern: ", pattern)
        for index, row in df.iterrows():
            rule = row["Rule"]
            interested_df = n_df[n_df["Rule"] == rule]
            values = {}
            for model in ["boxe", "complex", "hake", "hole", "quate", "rotate", "rotpro", "toruse", "transe"]:
                model_df = interested_df[interested_df["Model"] == model]
                values[model] = round(model_df["Support_DS_filtered"].values[0],2)
            for atom in rule.body_atoms:
                atom.relationship_name = id_to_relation[int(atom.relationship)]

            rule.head_atom.relationship_name = id_to_relation[int(rule.head_atom.relationship)]
            print("\t\tRule: ", row["Rule"].relationship_print(), "; boxe: ", values["boxe"], "; complex: ", values["complex"], "; hake: ", values["hake"], "; hole: ", values["hole"], "; quate: ", values["quate"], "; rotate: ", values["rotate"], "; rotpro: ", values["rotpro"], "; toruse: ", values["toruse"], "; transe: ", values["transe"])

    print("\n\n")


def convert_str_to_amie_rule(rule_str):
    body_atoms = []
    all_atoms = extract_rule(rule_str)
    for atom_structure in all_atoms[:-1]:
        atom = Atom(atom_structure[0], "?" + atom_structure[1], "?" + atom_structure[2], "")
        body_atoms.append(atom)

    head_atom = Atom(all_atoms[-1][0], "?" + all_atoms[-1][1], "?" + all_atoms[-1][2], "")

    rule = Rule(head_atom, body_atoms, 0.01, 0.01)

    return rule


def add_path_non_path(rules_by_pattern):
    rules_by_pattern["Transitive_path"] = {}
    rules_by_pattern["Transitive_non_path"] = {}
    rules_by_pattern["Composition_path"] = {}
    rules_by_pattern["Composition_non_path"] = {}
    for key in rules_by_pattern:
        if key in ["Transitive", "Composition"]:
            rules_by_pattern[key + "_path"] = []
            rules_by_pattern[key + "_non_path"] = []

            for _, row in rules_by_pattern[key].iterrows():
                rule = row[0]
                atom1, atom2 = rule.body_atoms

                if atom1.variable2 == atom2.variable1 or atom1.variable1 == atom2.variable2:
                    rules_by_pattern[key + "_path"].append(row)
                else:
                    rules_by_pattern[key + "_non_path"].append(row)

    rules_by_pattern["Transitive_path"] = pd.DataFrame(rules_by_pattern["Transitive_path"],
                                                       columns=rules_by_pattern["Transitive"].columns)
    rules_by_pattern["Transitive_non_path"] = pd.DataFrame(rules_by_pattern["Transitive_non_path"],
                                                           columns=rules_by_pattern["Transitive"].columns)
    rules_by_pattern["Composition_path"] = pd.DataFrame(rules_by_pattern["Composition_path"],
                                                        columns=rules_by_pattern["Composition"].columns)
    rules_by_pattern["Composition_non_path"] = pd.DataFrame(rules_by_pattern["Composition_non_path"],
                                                            columns=rules_by_pattern["Composition"].columns)
    return rules_by_pattern


def aggregate_inference_patterns(rules):
    results = []
    for pattern in rules:
        print(pattern)
        for model_name in rules[pattern]:

            rules_to_add = []
            for rule in rules[pattern][model_name]:
                rules_to_add.append(
                    [rule[0], int(rule[2]), float(rule[17]), float(rule[18]), float(rule[19]), float(rule[20]),
                     float(rule[21]), float(rule[22]), float(rule[23]), float(rule[24]),
                     float(rule[25]), float(rule[26]), float(rule[27]), float(rule[28])])

            df = pd.DataFrame(rules_to_add,
                              columns=["Rule", "K", "Support_JC", "Support_DS", "Support_recall", "Support_JC_filtered",
                                       "Support_DS_filtered", "Support_recall_filtered", "PCA_JC", "PCA_DS",
                                       "PCA_recall",
                                       "PCA_JC_filtered", "PCA_DS_filtered", "PCA_recall_filtered"])

            results_to_add = []
            k_values = set(df["K"])
            for k in k_values:
                required_df = df[(df["K"] == k)]
                results_to_add.append(
                    [k, required_df["Support_JC"].mean(), required_df["Support_DS"].mean(),
                     required_df["Support_recall"].mean(), required_df["Support_JC_filtered"].mean(),
                     required_df["Support_DS_filtered"].mean(), required_df["Support_recall_filtered"].mean(),
                     required_df["PCA_JC"].mean(), required_df["PCA_DS"].mean(), required_df["PCA_recall"].mean(),
                     required_df["PCA_JC_filtered"].mean(), required_df["PCA_DS_filtered"].mean(),
                     required_df["PCA_recall_filtered"].mean()])

            aggregated_df = pd.DataFrame(results_to_add,
                                         columns=["K", "Support_JC", "Support_DS", "Support_recall",
                                                  "Support_JC_filtered",
                                                  "Support_DS_filtered",
                                                  "Support_recall_filtered", "PCA_JC", "PCA_DS", "PCA_recall",
                                                  "PCA_JC_filtered", "PCA_DS_filtered",
                                                  "PCA_recall_filtered"])

            aggregated_k_0 = aggregated_df[(aggregated_df["K"] == 1)]
            aggregated_k_10 = aggregated_df[(aggregated_df["K"] == 10)]

            if len(aggregated_k_0) > 0 and len(aggregated_k_10) > 0:
                sup_JC_diff = aggregated_k_0["Support_JC"].values.tolist()[0] - \
                              aggregated_k_10["Support_JC"].values.tolist()[0]
                sup_DS_diff = aggregated_k_0["Support_DS"].values.tolist()[0] - \
                              aggregated_k_10["Support_DS"].values.tolist()[0]
                sup_AN_diff = aggregated_k_0["Support_recall"].values.tolist()[0] - \
                              aggregated_k_10["Support_recall"].values.tolist()[0]
                sup_JC_filtered_diff = aggregated_k_0["Support_JC_filtered"].values.tolist()[0] - \
                                       aggregated_k_10["Support_JC_filtered"].values.tolist()[0]
                sup_DS_filtered_diff = aggregated_k_0["Support_DS_filtered"].values.tolist()[0] - \
                                       aggregated_k_10["Support_DS_filtered"].values.tolist()[0]
                sup_AN_filtered_diff = aggregated_k_0["Support_recall_filtered"].values.tolist()[0] - \
                                       aggregated_k_10["Support_recall_filtered"].values.tolist()[0]
                pca_JC_diff = aggregated_k_0["PCA_JC"].values[0] - aggregated_k_10["PCA_JC"].values.tolist()[0]
                pca_DS_diff = aggregated_k_0["PCA_DS"].values[0] - aggregated_k_10["PCA_DS"].values.tolist()[0]
                pca_AN_diff = aggregated_k_0["PCA_recall"].values[0] - aggregated_k_10["PCA_recall"].values.tolist()[0]
                pca_JC_filtered_diff = aggregated_k_0["PCA_JC_filtered"].values.tolist()[0] - \
                                       aggregated_k_10["PCA_JC_filtered"].values.tolist()[0]
                pca_DS_filtered_diff = aggregated_k_0["PCA_DS_filtered"].values.tolist()[0] - \
                                       aggregated_k_10["PCA_DS_filtered"].values.tolist()[0]
                pca_AN_filtered_diff = aggregated_k_0["PCA_recall_filtered"].values.tolist()[0] - \
                                       aggregated_k_10["PCA_recall_filtered"].values.tolist()[0]

                results.append(
                    [pattern, model_name, sup_JC_diff, sup_DS_diff, sup_AN_diff, sup_JC_filtered_diff,
                     sup_DS_filtered_diff,
                     sup_AN_filtered_diff, pca_JC_diff, pca_DS_diff, pca_AN_diff, pca_JC_filtered_diff,
                     pca_DS_filtered_diff, pca_AN_filtered_diff])

    difference_df = pd.DataFrame(results,
                                 columns=["Pattern", "Model", "Support_JC", "Support_DS", "Support_recall",
                                          "Support_JC_filtered",
                                          "Support_DS_filtered", "Support_recall_filtered", "PCA_JC", "PCA_DS",
                                          "PCA_recall",
                                          "PCA_JC_filtered",
                                          "PCA_DS_filtered", "PCA_recall_filtered"])
    patterns = set(difference_df["Pattern"])
    for pattern in patterns:
        print(difference_df[(difference_df["Pattern"] == pattern)])


def make_latex_inputs(data):
    str_to_write = "&"
    models = ["boxe", "complex", "hake", "hole", "quate", "rotate", "rotpro", "toruse", "transe"]

    for model in models:
        str_to_write += model + "&"

    str_to_write = str_to_write[:-1] + "\\\\ \\hline \\hline \n"

    for pattern in data:
        if pattern == "Intersection":
            continue
        str_to_write += pattern + "&"
        for model in models:
            value = data[pattern][model]
            str_to_write += " \\begin{tabular}{@{}c@{}}" + str(value[0]) + " \\\\ " + str(value[1]) + "\\end{tabular}&"

        str_to_write = str_to_write[:-1] + "\\\\ \\hline \\hline \n"
    print(str_to_write)


def get_count_triples_in_test(path_to_data, dataset):
    triples_in_test = {}
    with open(f"{path_to_data}/{dataset}/test2id.txt") as f:
        f.readline()
        for line in f:
            splits = line.strip().split("\t")
            if len(splits) == 1:
                splits = line.strip().split(" ")

            head = int(splits[2])
            if head not in triples_in_test:
                triples_in_test[head] = 0
            triples_in_test[head] += 1

    return triples_in_test


def get_hc_weights(df):
    return float(df["HC_dataset"])


def get_same_weights(df):
    vals = [1 for i in range(len(df))]
    return vals


def get_rules_with_weights(df):
    weights_hc_values = []
    for index, row in df.iterrows():
        weights_hc_values.append(get_hc_weights(row))

    df["Weights_HC"] = weights_hc_values
    df["Weights_same"] = get_same_weights(df)
    return df


def read_named_pattern_files(path_to_rules, datasets, models, columns_to_keep, is_asymmetry=False,
                             is_intersection=False):
    rules = {}
    for dataset in datasets:
        for model in models:
            if is_asymmetry:
                path_to_named_patterns = f"{path_to_rules}/{dataset}/{model}/{dataset}_{model}_A_named_patterns.tsv"
            elif is_intersection:
                path_to_named_patterns = f"{path_to_rules}/{dataset}/{model}/{dataset}_{model}_I_named_patterns.tsv"
            else:
                path_to_named_patterns = f"{path_to_rules}/{dataset}/{model}/{dataset}_{model}_named_patterns.tsv"

            with open(path_to_named_patterns) as f:
                columns = f.readline().strip().split("\t")
                columns.append("Dataset")
                columns.append("Model")
                # print(dataset,model, columns)
                for line in f:
                    vals = line.strip().split("\t")
                    for i in range(3, len(vals)):
                        vals[i] = float(vals[i])
                    vals.append(dataset)
                    vals.append(model)

                    pattern = vals[1]
                    if pattern not in rules:
                        rules[pattern] = []

                    vals[0] = convert_str_to_amie_rule(vals[0])
                    rules[pattern].append(vals)

    rules_dfs_by_patterns = {}
    for pattern in rules:
        rules_dfs_by_patterns[pattern] = pd.DataFrame(rules[pattern], columns=columns)

        rules_dfs_by_patterns[pattern] = rules_dfs_by_patterns[pattern][
            rules_dfs_by_patterns[pattern]["Support_D"] >= 10]

    for pattern in rules:
        rules_dfs_by_patterns[pattern] = rules_dfs_by_patterns[pattern][columns_to_keep]

    for pattern in rules:
        rules_dfs_by_patterns[pattern] = get_rules_with_weights(rules_dfs_by_patterns[pattern])
    return rules_dfs_by_patterns


def get_k_dataframes(df):
    k_dataframes = {}
    for k in df["K"].unique():
        k_dataframes[k] = df[df["K"] == k]
    return k_dataframes


def weighted_arithmetic_mean(metrics: list, weights: list):
    result = 0
    for i in range(len(metrics)):
        result += metrics[i] * weights[i]
    return result / sum(weights)


def arithmetic_mean(metrics: list, weights=None):
    result = 0
    for i in range(len(metrics)):
        result += metrics[i]
    return result / len(metrics)


def weighted_geometric_mean(metrics: list, weights: list):
    result = 0
    for i in range(len(metrics)):
        result += math.log(metrics[i] + 0.0000001) * weights[i]

    return math.exp(result * 1.0 / sum(weights))


def aggregate_patterns(rules_by_pattern, aggregation_function, weights_column):
    aggregations = {}
    for pattern in rules_by_pattern:

        aggregations[pattern] = {}
        for model in rules_by_pattern[pattern]["Model"].unique():
            k_dataframes = get_k_dataframes(rules_by_pattern[pattern][rules_by_pattern[pattern]["Model"] == model])
            aggregated_k = {}
            for k in k_dataframes:
                result = {}
                df = k_dataframes[k]
                for column in df.columns:
                    if column not in ["Rule", "Dataset", "Model", "K", "Pattern"]:
                        result[f"{column}_aggregated"] = aggregation_function(df[column].values.tolist(),
                                                                              df[weights_column].values.tolist())

                aggregated_k[k] = result

            aggregations[pattern][model] = aggregated_k

    return aggregations


def compute_differences(aggregations, k1, k2):
    difference_dict = {}
    for pattern in aggregations:
        difference_dict[pattern] = {}
        for model in aggregations[pattern]:
            difference_dict[pattern][model] = {}
            if k1 not in aggregations[pattern][model] or k2 not in aggregations[pattern][model]:
                continue
            k_dict1 = aggregations[pattern][model][k1]
            k_dict2 = aggregations[pattern][model][k2]
            for key in k_dict1:
                difference_dict[pattern][model][key] = [round(k_dict1[key], 2), round(k_dict1[key] - k_dict2[key], 2)]
    return difference_dict


def convert_difference_to_latex_format(difference_dict, support_column, pca_column, models):
    result = {}
    for pattern in difference_dict:
        result[pattern] = {}
        if len(difference_dict[pattern]) == 0:
            for model in models:
                result[pattern][model] = ["-", "-"]
            continue
        for model in difference_dict[pattern]:
            if len(difference_dict[pattern][model]) == 0:
                result[pattern][model] = ["-", "-"]
                continue
            # print(pattern, model, difference_dict[pattern][model])
            result_support = f"{difference_dict[pattern][model][support_column][0]} ({difference_dict[pattern][model][support_column][1]})"
            result_pca = f"{difference_dict[pattern][model][pca_column][0]} ({difference_dict[pattern][model][pca_column][1]})"
            result[pattern][model] = [result_support, result_pca]

    return result


def aggregate_by_dataset(rules_by_pattern, datasets):
    result = {}
    for dataset in datasets:
        result[dataset] = {}
        for pattern in rules_by_pattern:
            df = rules_by_pattern[pattern]
            df = df[(df["Dataset"] == dataset)]
            if pattern not in result[dataset]:
                result[dataset][pattern] = {}
            result[dataset][pattern] = df

    return result


def aggregate_by_model_and_k(rules_by_pattern, aggregation_function, weights_column, support_col, pca_col, columns):
    df = pd.DataFrame(columns=columns)
    for pattern in rules_by_pattern:
        rules = rules_by_pattern[pattern]
        df = df.append(rules, ignore_index=True)

    models = df["Model"].unique()
    k_values = df["K"].unique()
    result = {}
    # print(df.columns)
    for k in k_values:
        result[k] = {}
        for model in models:
            df_model = df[(df["Model"] == model) & (df["K"] == k)]
            result[k][model] = [round(
                aggregation_function(df_model[support_col].values.tolist(), df_model[weights_column].values.tolist()),
                2),
                round(aggregation_function(df_model[pca_col].values.tolist(),
                                           df_model[weights_column].values.tolist()), 2)]

    return result


def create_positive_and_negative_table(data, is_pca=False):
    str_to_write = "&"
    models = ["boxe", "complex", "hake", "hole", "quate", "rotate", "rotpro", "toruse", "transe"]

    for model in models:
        str_to_write += model + "&"

    str_to_write = str_to_write[:-1] + "\\\\ \\hline \\hline \n"

    for pattern in data:

        str_to_write += pattern + "&"
        for model in models:
            value = data[pattern][model]
            if model not in data[pattern]:
                str_value = "-"
            else:
                if not is_pca:
                    str_value = str(value[0])
                else:
                    str_value = str(value[1])
            str_to_write += " " + str_value.replace("0.", ".") + "&"

        str_to_write = str_to_write[:-1] + "\\\\ \\hline \\hline \n"
    print(str_to_write)


def get_id_to_relation_mapping(path_to_dataset, dataset):
    id_to_relation = {}

    with open(f"{path_to_dataset}/{dataset}/relation2id.txt") as f:
        f.readline()

        for line in f:
            relation, id = line.strip().split("\t")
            id_to_relation[int(id)] = relation

    return id_to_relation


if __name__ == "__main__":
    # datasets = sys.argv[2].split(",")
    # models = sys.argv[3].split(",")
    # path_to_data = sys.argv[1]
    # k = sys.argv[4].split(",")
    path_to_data = "D:\PhD\Work\Phd_research\InferenceMinedRules\\"
    path_to_dataset = "D:\PhD\Work\EmbeddingInterpretibility\Interpretibility\Datasets"
    datasets = ["WN18", "WN18RR", "YAGO3-10"]

    models = ["boxe", "complex", "hake", "hole", "quate", "rotate", "rotpro", "toruse", "transe"]
    k = [1, 5, 10, 25, 50]

    path_to_rules = f"{path_to_data}/MinedRules"

    columns = []

    columns_to_keep = ["Rule", "Pattern", "Dataset", "Model", "K", "HC_dataset", "Support_JC", "Support_DS",
                       "Support_recall",
                       "Support_JC_filtered",
                       "Support_DS_filtered", "Support_recall_filtered",
                       "PCA_JC", "PCA_DS", "PCA_recall", "PCA_JC_filtered", "PCA_DS_filtered", "PCA_recall_filtered"]
    rules_by_patterns = read_named_pattern_files(path_to_rules, datasets, models, columns_to_keep, is_asymmetry=False)
    asymmetry_rules = read_named_pattern_files(path_to_rules, datasets, models, columns_to_keep, is_asymmetry=True)
    intersection_rules = read_named_pattern_files(path_to_rules, datasets, models, columns_to_keep,
                                                  is_intersection=True)
    rules_by_patterns = {**rules_by_patterns, **asymmetry_rules, **intersection_rules}

    for pattern in rules_by_patterns:
        rules_by_patterns[pattern][columns_to_keep[5:]] = rules_by_patterns[pattern][columns_to_keep[5:]].apply(
            pd.to_numeric, errors='coerce')
    # print(rules_by_patterns)

    # all_rules = add_path_non_path(rules_by_patterns)
    all_rules = rules_by_patterns
    rules_by_dataset = aggregate_by_dataset(all_rules, datasets)
    # print(rules_by_dataset)

    for dataset in rules_by_dataset:

        # result = aggregate_by_model_and_k(rules_by_dataset[dataset], weighted_arithmetic_mean, "Weights_same",
        #                                   "Support_JC_filtered",
        #                                   "PCA_JC_filtered", columns_to_keep)
        # print("Dataset: ", dataset)
        # print("Support")
        # create_positive_and_negative_table(result, is_pca=False)
        # print("PCA")
        # create_positive_and_negative_table(result, is_pca=True)
        pattern_dict = rules_by_dataset[dataset]
        id_to_relation = get_id_to_relation_mapping(path_to_dataset, dataset)
        get_best_rules(pattern_dict, 5, dataset, id_to_relation)
        # get_best_avg_rules(pattern_dict, 5, dataset, id_to_relation)
        # aggregated_patterns = aggregate_patterns(pattern_dict, arithmetic_mean, "Weights_same")
        #
        # difference_dict = compute_differences(aggregated_patterns, "1", "10")
        # result_latex_format = convert_difference_to_latex_format(difference_dict, "Support_DS_filtered_aggregated",
        #                                                          "PCA_DS_filtered_aggregated", models)
        # print("Dataset: ", dataset)
        # print("Support")
        # create_positive_and_negative_table(result_latex_format)
        # print("PCA")
        # create_positive_and_negative_table(result_latex_format, is_pca=True)
        #
        # print("\n\n")
