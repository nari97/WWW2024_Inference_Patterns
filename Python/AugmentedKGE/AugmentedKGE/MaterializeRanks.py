import sys
import torch
from DataLoader.TripleManager import TripleManager
from Train.RankMaterializer import RankMaterializer
import pickle


def materialize(dataset_name, model_name, folder_to_dataset, folder_to_model):
    """
    This function is used to materialize the top k ranked triples for a given link prediction model and dataset.
    Args:
        dataset_name: The name of the dataset
        model_name: The name of the model
        folder_to_dataset: Path to the folder containing the dataset
        folder_to_model: Path to the folder containing the model

    Returns:

    """
    split_prefix, point = '', 0
    rel_anomaly_min = 0
    rel_anomaly_max = 1.0
    corruption_mode = "LCWA"

    print("Model:", model_name, "; Dataset:", dataset_name, "; Corruption:", corruption_mode)

    path = folder_to_dataset + "/" + dataset_name + "/"
    triple_to_id = pickle.load(open(f"{folder_to_dataset}//{dataset_name}//{dataset_name}_triple_to_id.pkl", "rb"))
    manager = TripleManager(path, splits=[split_prefix + "test", split_prefix + "valid", split_prefix + "train"],
                            corruption_mode=corruption_mode)
    model_path = f"{folder_to_model}/{dataset_name}/{model_name}/{dataset_name}_{model_name}.model"
    model = torch.load(model_path)

    evaluator = RankMaterializer(manager, rel_anomaly_max=rel_anomaly_max, rel_anomaly_min=rel_anomaly_min,
                                 batched=False,
                                 triple_to_id=triple_to_id)

    evaluator.collect_ranks(model,
                            materialize_basefile=f"{folder_to_model}/{dataset_name}/{model_name}/{dataset_name}_{model_name}")


if __name__ == "__main__":
    model_name = sys.argv[1]
    dataset_name = sys.argv[2]
    folder_to_datasets = sys.argv[3]
    folder_to_model = sys.argv[4]

    materialize(model_name=model_name, dataset_name=dataset_name, folder_to_dataset=folder_to_datasets,
                folder_to_model=folder_to_model)
