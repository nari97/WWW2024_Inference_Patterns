from .Rule import Rule
from .Atom import Atom


def extract_rule(rule_str):
    input_list = rule_str.split(' ')
    output_list = []
    for item in input_list:
        if '==>' in item:
            continue

        if "?" not in item:
            num, variable1, variable2 = item.replace(')', '').replace('(', ',').split(',')
        else:
            num, variable1, variable2 = item.replace(')', '').replace('(', '').split('?')
        output_list.append([int(num), variable1.replace(",", ''), variable2.replace(',', '')])

    return output_list


def construct_rule_from_list(inp_list):
    body_atoms = []
    all_atoms = inp_list

    for atom_structure in all_atoms[:-1]:
        atom = Atom(atom_structure[0], "?" + atom_structure[1], "?" + atom_structure[2], "")
        body_atoms.append(atom)

    head_atom = Atom(all_atoms[-1][0], "?" + all_atoms[-1][1], "?" + all_atoms[-1][2], "")

    rule = Rule(head_atom, body_atoms)
    return rule

def compute_selectivity(row, beta=1.0):
    selectivity = ((1 + beta * beta) * row["PCA Confidence"] * row["Head Coverage"]) / (
                    beta * beta * row["PCA Confidence"] + row["Head Coverage"])

    return selectivity

def get_best_rule_per_predicate_by_selectivity(rules_df):
    df = rules_df
    df["Selectivity"] = df.apply(compute_selectivity, axis=1)

    result_df = df.sort_values(by=["Selectivity"], ascending=False).groupby("Head Predicate").head(1)
    return result_df