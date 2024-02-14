import pickle
import sys


def remove_newline_character_from_last_line(path_to_file):
    """
    This function removes the newline character from the last line of a file.
    Args:
        path_to_file: Path to the file

    Returns:

    """
    with open(path_to_file, 'r') as f:
        contents = f.read()

    # Remove the newline character from the last line
    contents = contents.rstrip('\n')

    # Write the contents back to the file
    with open(path_to_file, 'w') as f:
        f.write(contents)


def count_lines_in_file(path_to_file):
    """
    This function counts the number of lines in a file.
    Args:
        path_to_file: Path to the file

    Returns:
        The number of lines in the file
    """
    with open(path_to_file) as f:
        for i, l in enumerate(f):
            pass
    return i + 1


def filter_triples_with_score_less_than_min_rank(path_to_materialization, path_to_result, min_ranks: list):
    """
    This function takes in a materialization file and filters out the triples that have a rank less than or equal to min_ranks.
    Args:
        path_to_materialization: Path to the materialization file
        path_to_result: Path to the result file
        min_ranks: List of scores to collect

    Returns:

    """
    triple_dict = pickle.load(open(path_to_materialization, "rb"))

    files_dict = {k: open(f"{path_to_result}_filtered_{k}.tsv", "w+") for k in min_ranks}

    triple_counter = {}
    for k in min_ranks:
        triple_counter[k] = [0,0]

    for key, value in triple_dict.items():
        head_rank = 999
        tail_rank = 999
        pos_rank = 999
        if len(value[0]) == 0 and len(value[1]) == 0:
            if len(value[2]) > 0 and len(value[3])> 0:
                # print(key, value)
                pos_rank = min(value[2][0][1], value[3][0][1])
            elif len(value[2]) == 0:
                pos_rank = value[3][0][1]
            else:
                pos_rank = value[2][0][1]

        if len(value[0]) > 0:
            head_rank = min(value[0], key=lambda x: x[0])[0]
        if len(value[1]) > 0:
            tail_rank = min(value[1], key=lambda x: x[0])[0]
        for rank_key in files_dict:
            # print(head_rank, " ", tail_rank, " ", rank_key)
            if head_rank <= rank_key or tail_rank <= rank_key or pos_rank <= rank_key:
                if pos_rank <= rank_key:
                    triple_counter[rank_key][0] += 1
                else:
                    triple_counter[rank_key][1] += 1
                files_dict[rank_key].write("%s\t%s\t%s\n" % (key[0], key[1], key[2]))

    for key in files_dict:
        files_dict[key].close()

    for key in files_dict:
        remove_newline_character_from_last_line(f"{path_to_result}_filtered_{key}.tsv")

    for key in files_dict:
        print(
            f"Number of negative triples with rank less than or equal to {key}: {triple_counter[key][1]}")
        print(
            f"Number of positive triples with rank less than or equal to {key}: {triple_counter[key][0]}")


if __name__ == "__main__":
    path_to_materialization = sys.argv[1]
    path_to_result = sys.argv[2]
    ranks = sys.argv[3]

    min_ranks = []

    for splits in ranks.strip().split(","):
        min_ranks.append(int(splits))

    filter_triples_with_score_less_than_min_rank(path_to_materialization, path_to_result, min_ranks)
    print("Filtering finished")
