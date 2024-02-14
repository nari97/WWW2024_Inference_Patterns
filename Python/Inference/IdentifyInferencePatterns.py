import os
import sys

import pandas as pd

from ParseRules import Atom, Rule

pd.set_option('display.max_columns', None)


def extract_rule(rule_str):
    input_list = rule_str.split(' ')
    output_list = []
    for item in input_list:
        if '=>' in item:
            continue

        num, variable1, variable2 = item.replace(')', '').replace('(', '').split('?')
        output_list.append([int(num), variable1.replace(",", ''), variable2.replace(',', '')])

    return output_list


def convert_rules_to_dataframe(rules_file_path, columns):
    rules = []
    with open(rules_file_path, "r") as f:
        f.readline()
        for line in f:
            line = line.strip()
            splits = line.split("\t")
            rule_str = splits[0]
            hc_dataset = float(splits[1])
            pca_dataset = float(splits[2])
            hc_predictions = float(splits[3])
            pca_predictions = float(splits[4])
            hc_adjusted = float(splits[5])
            pca_adjusted = float(splits[6])
            sup_D = float(splits[7])
            sup_P = float(splits[8])
            sup_D_P = float(splits[9])
            sup_P_D = float(splits[10])
            pca_D = float(splits[11])
            pca_P = float(splits[12])
            pca_D_P = float(splits[13])
            pca_P_D = float(splits[14])
            sup_JC = float(splits[15])
            sup_DS = float(splits[16])
            sup_recall = float(splits[17])
            sup_JC_filtered = float(splits[18])
            sup_DS_filtered = float(splits[19])
            sup_recall_filtered = float(splits[20])
            pca_JC = float(splits[21])
            pca_DS = float(splits[22])
            pca_recall = float(splits[23])
            pca_JC_filtered = float(splits[24])
            pca_DS_filtered = float(splits[25])
            pca_recall_filtered = float(splits[26])
            body_atoms = []
            all_atoms = extract_rule(rule_str)

            for atom_structure in all_atoms[:-1]:
                atom = Atom(atom_structure[0], "?" + atom_structure[1], "?" + atom_structure[2], "")
                body_atoms.append(atom)

            head_atom = Atom(all_atoms[-1][0], "?" + all_atoms[-1][1], "?" + all_atoms[-1][2], "")

            rule = Rule(head_atom, body_atoms, 0.01, 0.01)
            rules.append(
                [rule, hc_dataset, pca_dataset, hc_predictions, pca_predictions, hc_adjusted, pca_adjusted, sup_D,
                 sup_P, sup_D_P,
                 sup_P_D, pca_D, pca_P, pca_D_P, pca_P_D, sup_JC, sup_DS, sup_recall, sup_JC_filtered, sup_DS_filtered,
                 sup_recall_filtered,
                 pca_JC, pca_DS, pca_recall, pca_JC_filtered, pca_DS_filtered, pca_recall_filtered])

    rules_df = pd.DataFrame(rules, columns=columns)
    return rules_df


def aggregate_inference_patterns(rules_df):
    symmetry = pd.DataFrame(columns=rules_df.columns)
    hierarchy = pd.DataFrame(columns=rules_df.columns)
    intersection = pd.DataFrame(columns=rules_df.columns)
    composition = pd.DataFrame(columns=rules_df.columns)
    transitive = pd.DataFrame(columns=rules_df.columns)
    inversion = pd.DataFrame(columns=rules_df.columns)

    for index, row in rules_df.iterrows():
        rule = row["Rule"]
        # print("Rule: ", rule)
        if len(rule.body_atoms) == 1:
            atom1 = rule.body_atoms[0]
            atom2 = rule.head_atom
            if atom1.relationship == atom2.relationship:
                if atom1.variable1 == atom2.variable2 and atom1.variable2 == atom2.variable1:
                    symmetry = symmetry.append(row, ignore_index=True)
            else:
                if atom1.variable1 == atom2.variable1 and atom1.variable2 == atom2.variable2:

                    hierarchy = hierarchy.append(row, ignore_index=True)
                elif atom1.variable1 == atom2.variable2 and atom1.variable2 == atom2.variable1:

                    inversion = inversion.append(row, ignore_index=True)

        else:
            atom1, atom2 = rule.body_atoms
            atom3 = rule.head_atom

            if atom1.relationship != atom2.relationship != atom3.relationship:
                if atom1.variable1 == atom2.variable1 == atom3.variable1 and atom1.variable2 == atom2.variable2 == atom3.variable2:

                    intersection = intersection.append(row, ignore_index=True)
                else:

                    composition = composition.append(row, ignore_index=True)
            else:
                if atom1.relationship == atom2.relationship == atom3.relationship:

                    transitive = transitive.append(row, ignore_index=True)
                else:
                    composition = composition.append(row, ignore_index=True)

    # if len(symmetry) > 0:
    #     print(f"Symmetry present; length: {len(symmetry)}; example: {symmetry.loc[0]['Rule'].id_print()}")
    #
    # if len(hierarchy) > 0:
    #     print(f"Hierarchy present; length: {len(hierarchy)}; example: {hierarchy.loc[0]['Rule'].id_print()}")
    #
    # if len(composition) > 0:
    #     print(f"Composition present; length: {len(composition)}; example: {composition.loc[0]['Rule'].id_print()}")
    #
    # if len(intersection) > 0:
    #     print(f"Intersection present; length: {len(intersection)}; example: {intersection.loc[0]['Rule'].id_print()}")
    #
    # if len(transitive) > 0:
    #     print(f"Transitive present; length: {len(transitive)}; example: {transitive.loc[0]['Rule'].id_print()}")

    return {"Symmetry": symmetry, "Hierarchy": hierarchy, "Composition": composition, "Intersection": intersection,
            "Transitive": transitive, "Inversion": inversion}


def segment_df_path_non_path(df):
    non_path_df = pd.DataFrame(columns=df.columns)
    path_df = pd.DataFrame(columns=df.columns)

    for index, row in df.iterrows():
        rule = row["Rule"]
        atom1 = rule.body_atoms[0]
        atom2 = rule.body_atoms[1]
        if atom1.variable2 == atom2.variable1:
            path_df = path_df.append(row, ignore_index=True)
        else:
            non_path_df = non_path_df.append(row, ignore_index=True)

    return path_df, non_path_df


def summarize_df(df):
    return df["Support_JC"].mean(), df["Support_DS"].mean(), df["Support_AN"].mean(), df["Support_JC_filtered"].mean(), \
           df["Support_DS_filtered"].mean(), df["Support_AN_filtered"].mean(), df["PCA_JC"].mean(), df["PCA_DS"].mean(), \
           df["PCA_AN"].mean(), df["PCA_JC_filtered"].mean(), df["PCA_DS_filtered"].mean(), df["PCA_AN_filtered"].mean()


def summarize_adjusted(df):
    return df["HC_adjusted"].mean(), df["PCA_adjusted"].mean()


def summarize_set_difference_support(df):
    return df["Support_D"].mean(), df["Support_P"].mean(), df["Support_D_P"].mean(), df["Support_P_D"].mean()


def summarize_set_difference_pca(df):
    return df["PCA_D"].mean(), df["PCA_P"].mean(), df["PCA_D_P"].mean(), df["PCA_P_D"].mean()


def is_matching_file(file_name, dataset_name, model_name, k, is_asymmetry, split_id):
    # print(file_name, file_name.startswith(f"{dataset_name}_{model_name}_filtered_rules_inference_{k}"))
    if is_asymmetry:
        return file_name.startswith(f"{dataset_name}_{model_name}_filtered_rules_inference_asymmetry_{k}_")
    else:
        return file_name.startswith(f"{dataset_name}_{model_name}_filtered_rules_inference_{k}_") and \
                   file_name.endswith(f"_split_{split_id}.tsv")


if __name__ == "__main__":
    path_to_data = sys.argv[1]
    dataset_name = sys.argv[2]
    model_name = sys.argv[3]
    k = sys.argv[4]
    split_id = sys.argv[5]
    k = k.strip().split(",")
    # is_asymmetry = True if sys.argv[5] == "1" else False
    path_to_file = f"{path_to_data}/MinedRules/{dataset_name}/{model_name}/{dataset_name}_{model_name}"
    # List files in the folder and filter by the pattern
    result_files = [path_to_file + f"_named_patterns_split_{split_id}.tsv"]
    # result_files = [path_to_file + f"_named_patterns.tsv", path_to_file + f"_A_named_patterns.tsv"]
    # if is_asymmetry:
    #     result_file = f"{path_to_data}/MinedRules/{dataset_name}/{model_name}/{dataset_name}_{model_name}_A_named_patterns.tsv"
    # else:
    #     result_file = f"{path_to_data}/MinedRules/{dataset_name}/{model_name}/{dataset_name}_{model_name}_named_patterns.tsv"
    for result_file in result_files:
        if "_A_" in result_file:
            is_asymmetry = True
        else:
            is_asymmetry = False
        f = open(result_file, "w+")
        cols = ["Rule", "HC_dataset", "PCA_dataset", "HC_predictions",
                "PCA_predictions", "HC_adjusted", "PCA_adjusted", "Support_D", "Support_P", "Support_D_P",
                "Support_P_D",
                "PCA_D", "PCA_P", "PCA_D_P", "PCA_P_D", "Support_JC", "Support_DS", "Support_recall",
                "Support_JC_filtered",
                "Support_DS_filtered", "Support_recall_filtered",
                "PCA_JC", "PCA_DS", "PCA_recall", "PCA_JC_filtered", "PCA_DS_filtered", "PCA_recall_filtered"]

        f.write("Rule\tPattern\tK\t" + "\t".join(cols[1:]) + "\n")
        result_list = []
        for k_value in k:
            matching_files = [file for file in os.listdir(f"{path_to_data}/MinedRules/{dataset_name}/{model_name}") if
                              is_matching_file(file, dataset_name, model_name, k_value, is_asymmetry, split_id)]
            print(f"Found {matching_files} matching files for k={k_value}")
            rules_df = pd.DataFrame()

            # Loop through matching files and concatenate dataframes
            for file in matching_files:
                file_path = f"{path_to_data}/MinedRules/{dataset_name}/{model_name}/{file}"

                # Assuming convert_rules_to_dataframe returns a DataFrame
                current_df = convert_rules_to_dataframe(file_path, cols)

                # Concatenate the current dataframe to the existing rules_df
                rules_df = pd.concat([rules_df, current_df], ignore_index=True)

            if is_asymmetry:
                inference_patterns = {"Asymmetry": rules_df}
            else:
                inference_patterns = aggregate_inference_patterns(rules_df)

            for key in inference_patterns:
                print(f"Number of rules in pattern={key} is {len(inference_patterns[key])}")
            result = {}

            for key in inference_patterns:
                for index, row in inference_patterns[key].iterrows():
                    rule = row["Rule"]
                    string_to_write = f"{rule.id_print()}\t{key}\t{k_value}\t"
                    row_list = row.tolist()
                    string_to_write += "\t".join([str(item) for item in row_list[1:]])
                    string_to_write += "\n"
                    f.write(string_to_write)

        f.close()
    print("Identified inference patterns")
