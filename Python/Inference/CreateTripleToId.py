import pickle


def create_triple_to_id(path, dataset_name):
    """
    Create a dictionary mapping triples to ids and store it in a pickle file
    Args:
        path:  Path to the dataset folder
        dataset_name: Name of the dataset

    Returns:

    """

    triple_to_id = {}

    with open(f"{path}//{dataset_name}//test2id.txt") as f:

        f.readline()
        for line in f:
            # print(line)
            splits = line.strip().split("\t")
            if len(splits) == 1:
                splits = line.strip().split(" ")

            head, tail, relation = splits
            head, tail, relation = int(head), int(tail), int(relation)
            triple_to_id[(head, relation, tail)] = len(triple_to_id) + 1

    pickle.dump(triple_to_id, open(f"{path}//{dataset_name}//{dataset_name}_triple_to_id.pkl", "wb"))


if __name__ == "__main__":
    datasets = ["BioKG", "Hetionet"]
    path = r"D:\PhD\Work\EmbeddingInterpretibility\Interpretibility\Datasets"
    for dataset in datasets:
        create_triple_to_id(path=path, dataset_name=dataset)
