import random

def create_splits(path_to_folder, num_iterations=10):
    test_f = open(f"{path_to_folder}/test2id.txt", "r")
    valid_f = open(f"{path_to_folder}/valid2id.txt", "r")

    triples = []
    test_f.readline()
    for line in test_f:
        triples.append(line.strip())

    valid_f.readline()
    for line in valid_f:
        triples.append(line.strip())

    for ite in range(num_iterations):
        # Shuffle the triples randomly
        random.shuffle(triples)

        # Split the triples into two halves
        split_index = len(triples) // 2
        first_half = triples[:split_index]
        second_half = triples[split_index:]

        # Process the first half (you can modify this part according to your needs)
        with open(f"{path_to_folder}/valid_{ite}_2id.txt", "w") as first_half_file:
            first_half_file.write(f"{len(first_half)}\n")
            for triple in first_half:
                first_half_file.write(f"{triple}\n")

        # Process the second half (you can modify this part according to your needs)
        with open(f"{path_to_folder}/test_{ite}_2id.txt", "w") as second_half_file:
            second_half_file.write(f"{len(second_half)}\n")
            for triple in second_half:
                second_half_file.write(f"{triple}\n")

# Example usage
if __name__ == "__main__":
    create_splits("D:\\PhD\\Work\\Phd_research\\data\\Datasets\\WN18", num_iterations=10)
