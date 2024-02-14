import os
def create_amie_triples(folder_to_dataset, dataset_name):
    """

    Args:
        folder_to_dataset:
        dataset_name:

    Returns:

    """

    result_file = open(f"{folder_to_dataset}/{dataset_name}/{dataset_name}_AMIE_triples.tsv", "w+")
    for data_splits in ["train2id.txt", "valid2id.txt", "test2id.txt"]:
        with open(f"{folder_to_dataset}/{dataset_name}/{data_splits}", "r") as f:
            f.readline()
            for line in f:
                splits = line.strip().split("\t")
                if len(splits) == 1:
                    splits = line.strip().split(" ")
                result_file.write(f"{splits[0]}\t{splits[2]}\t{splits[1]}\n")


def run_amie(folder_to_dataset, dataset_name):
    """

    Args:
        folder_to_dataset:
        dataset_name:

    Returns:

    """
    path_to_amie = r"D:\PhD\Work\EmbeddingInterpretibility\Interpretibility\amie-dev.jar"
    path_to_amie_output = f"{folder_to_dataset}/{dataset_name}/{dataset_name}_rules.tsv"

    # The next lines need to execute java -jar on amie-dev.jar using os

    os.system(f"java -jar {path_to_amie} \"{folder_to_dataset}/{dataset_name}/{dataset_name}_AMIE_triples.tsv\" --datalog -minhc 0.1 > \"{path_to_amie_output}\"")


if __name__ == "__main__":
    folder_to_dataset = r"D:\PhD\Work\EmbeddingInterpretibility\Interpretibility\Datasets"
    dataset_name = "Hetionet"
    create_amie_triples(folder_to_dataset, dataset_name)
    run_amie(folder_to_dataset, dataset_name)
