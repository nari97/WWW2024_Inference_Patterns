import os
import re
import sys

def get_materialization_files(folder_to_copy, dataset, model, k):
    # List all files in the directory
    all_files = os.listdir(folder_to_copy)

    # Define the regular expression pattern
    pattern = rf'^{dataset}_{model}_.*_filtered_{k}\.tsv'

    # Filter the files based on the pattern
    filtered_files = [f for f in all_files if re.match(pattern, f)]

    return filtered_files

def get_rules_files(folder_to_copy, dataset, model, k):

    all_files = os.listdir(folder_to_copy)

    pattern = rf'^{dataset}_{model}_evaluated_inference_patterns.*{k}_.*\.tsv'

    filtered_files = [f for f in all_files if re.match(pattern, f)]

    return filtered_files

def create_dir_if_not_exists(path, dataset, model):
    if not os.path.exists(f"{path}"):
        os.makedirs(f"{path}")

    if not os.path.exists(f"{path}/{dataset}"):
        os.makedirs(f"{path}/{dataset}")

    if not os.path.exists(f"{path}/{dataset}/{model}"):
        os.makedirs(f"{path}/{dataset}/{model}")

def transfer_materializations(path_to_base, path_to_submission, dataset, models, k):
    model_cnt = 0
    file_cnt = 0

    for model in models:
        print(f"Copying materializations for model: {model}")
        folder_to_copy = f"{path_to_base}/Materializations/{dataset}/{model}"
        folder_to_paste = f"{path_to_submission}/Materializations/{dataset}/{model}"
        create_dir_if_not_exists(f"{path_to_submission}/Materializations", dataset, model)


        # print(f"Copying: {folder_to_copy}/{dataset}_{model}.model {folder_to_paste}/{dataset}_{model}.model")
        model_cnt += 1
        os.system(f"cp {folder_to_copy}/{dataset}_{model}.model {folder_to_paste}/{dataset}_{model}.model")

        files_to_copy = get_materialization_files(folder_to_copy, dataset, model, k)
        for file in files_to_copy:
            # print(f"Copying: {folder_to_copy}/{file} {folder_to_paste}/{file}")
            file_cnt += 1
            os.system(f"cp {folder_to_copy}/{file} {folder_to_paste}/{file}")

    print(f"Total models copied: {model_cnt}")
    print(f"Total materialization files copied: {file_cnt}")

def transfer_rules(path_to_base, path_to_submission, dataset, models, k):
    file_cnt = 0
    for model in models:
        print(f"Copying rules for model: {model}")
        folder_to_copy = f"{path_to_base}/MinedRules/{dataset}/{model}"
        folder_to_paste = f"{path_to_submission}/MinedRules/{dataset}/{model}"
        create_dir_if_not_exists(f"{path_to_submission}/MinedRules", dataset, model)
        files_to_copy = get_rules_files(folder_to_copy, dataset, model, k)
        for file in files_to_copy:
            # print(f"Copying: {folder_to_copy}/{file} {folder_to_paste}/{file}")
            file_cnt += 1
            os.system(f"cp {folder_to_copy}/{file} {folder_to_paste}/{file}")

    print(f"Total rules files copied: {file_cnt}")



if __name__ == "__main__":
    path_to_base = sys.argv[1]
    path_to_submission = sys.argv[2]
    dataset = sys.argv[3]

    models = ["boxe", "complex", "hake", "hole", "quate", "rotate", "rotpro", "toruse", "transe"]
    # models = ["boxe", "complex"]
    k=5

    print("Copying materializations")
    transfer_materializations(path_to_base, path_to_submission, dataset, models, k)
    print("\n")
    print("Copying rules")
    transfer_rules(path_to_base, path_to_submission, dataset, models, k)
