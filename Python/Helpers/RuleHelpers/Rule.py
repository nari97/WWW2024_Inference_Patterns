class Rule:
    def __init__(self, head_atom, body_atoms, head_coverage=None, pca_confidence=None, functional_variable=None):

        self.head_atom = head_atom
        self.body_atoms = body_atoms
        self.head_coverage = head_coverage
        self.pca_confidence = pca_confidence
        self.functional_variable = functional_variable

    def __hash__(self):
        return hash(self.id_print())

    def get_selectivity(self, beta):
        selectivity = 0.0
        if self.head_coverage is not None and self.pca_confidence is not None:
            selectivity = ((1 + beta * beta) * self.pca_confidence * self.head_coverage) / (
                    beta * beta * self.pca_confidence + self.head_coverage)

        return selectivity

    def create_atom_storage_structure(self):
        atom_storage = {}

        for atom in self.body_atoms:
            atom_storage[(atom.variable1, atom.variable2)] = atom.relationship

        atom_storage[(self.head_atom.variable1, self.head_atom.variable2)] = self.head_atom.relationship

        return atom_storage

    def __eq__(self, other):
        if self.id_print() == other.id_print():
            return True

        return False

    def id_print(self):

        str = ""

        for atom in self.body_atoms:
            str += atom.id_print() + " "

        str += "==>"

        str += self.head_atom.id_print()

        return str

    def relationship_print(self):
        str = ""

        for atom in self.body_atoms:
            str += atom.relationship_print() + " "

        str += "==>"

        str += self.head_atom.relationship_print()

        return str

    def print_metrics(self):
        return "Head Coverage: " + str(self.head_coverage) + ", PCA Confidence: " + str(self.pca_confidence) + \
               ", Selectivity: " + str(self.selectivity)

    def get_variables(self):

        variables = []
        for atom in self.body_atoms:
            v1, v2 = atom.get_variables()
            if v1 not in variables:
                variables.append(v1.replace("?", ""))
            if v2 not in variables:
                variables.append(v2.replace("?", ""))

        return variables

