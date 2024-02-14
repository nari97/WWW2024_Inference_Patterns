
def report_results(path_to_folder, datasets, models, k):
    datasets_mins = {}
    for dataset in datasets:
        dataset_min_hc= 999.0
        dataset_min_pca= 999.0
        dataset_min_support= 999999999999.0

        dataset_max_hc= 0.0
        dataset_max_pca= 0.0
        dataset_max_support= 0.0
        print(f"Dataset: {dataset}")
        print("-------------------------------------------------------")
        for model in models:
            model_min_hc = 999.0
            model_min_pca = 999.0
            model_min_support = 999999999999.0

            model_max_hc = 0.0
            model_max_pca = 0.0
            model_max_support = 0.0
            path_to_file = f"{path_to_folder}/{dataset}/{model}/{dataset}_{model}_evaluated_inference_patterns_intersection_{k}_split_10.tsv"
            # Concerned columns are: HC_dataset:3; PCA_dataset:4; |Support_D|: 9
            with open(path_to_file, "r") as file:
                file.readline()
                for line in file:
                    line = line.strip().split("\t")

                    model_min_hc = min(model_min_hc, float(line[3]))
                    model_min_pca = min(model_min_pca, float(line[4]))
                    model_min_support = min(model_min_support, float(line[9]))

                    dataset_min_hc = min(dataset_min_hc, model_min_hc)
                    dataset_min_pca = min(dataset_min_pca, model_min_pca)
                    dataset_min_support = min(dataset_min_support, model_min_support)

                    model_max_hc = max(model_max_hc, float(line[3]))
                    model_max_pca = max(model_max_pca, float(line[4]))
                    model_max_support = max(model_max_support, float(line[9]))

                    dataset_max_hc = max(dataset_max_hc, model_max_hc)
                    dataset_max_pca = max(dataset_max_pca, model_max_pca)
                    dataset_max_support = max(dataset_max_support, model_max_support)

                print(f"Model: {model}; Min HC: {model_min_hc} Max HC: {model_max_hc}; Min PCA: {model_min_pca} Max PCA: {model_max_pca}; Min Support_D: {model_min_support} Max Support_D: {model_max_support}")

        datasets_mins[dataset] = (dataset_min_hc, dataset_min_pca, dataset_min_support, dataset_max_hc, dataset_max_pca, dataset_max_support)
        print("")

    for dataset in datasets:
        # print(f"Dataset: {dataset}; Min HC: {datasets_mins[dataset][0]}; Min PCA: {datasets_mins[dataset][1]}; Min Support_D: {datasets_mins[dataset][2]}")
        print(f"Dataset: {dataset}; Min HC: {datasets_mins[dataset][0]} Max HC: {datasets_mins[dataset][3]}; Min PCA: {datasets_mins[dataset][1]} Max PCA: {datasets_mins[dataset][4]}; Min Support_D: {datasets_mins[dataset][2]} Max Support_D: {datasets_mins[dataset][5]}")

if __name__ == "__main__":
    path_to_folder = "D:\PhD\Work\Phd_research\data\RebuttalMinedRules"
    datasets = ["WN18", "WN18RR", "YAGO3-10", "BioKG", "Hetionet"]
    # datasets = ["WN18RR"]
    models = ["boxe", "complex", "hake", "hole", "quate", "rotate", "rotpro", "toruse", "transe"]
    k=5
    report_results(path_to_folder, datasets, models, k)