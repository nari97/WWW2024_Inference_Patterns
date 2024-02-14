import pandas as pd


def identify_inference_patterns_from_df(df):
    symmetry = pd.DataFrame(columns=df.columns)
    hierarchy = pd.DataFrame(columns=df.columns)
    intersection = pd.DataFrame(columns=df.columns)
    composition = pd.DataFrame(columns=df.columns)
    transitive = pd.DataFrame(columns=df.columns)
    inversion = pd.DataFrame(columns=df.columns)

    for index, row in df.iterrows():
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
