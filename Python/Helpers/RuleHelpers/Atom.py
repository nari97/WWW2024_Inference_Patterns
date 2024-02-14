class Atom:
    def __init__(self, relationship, variable1, variable2, relationship_name):
        self.placeholder = 1
        self.relationship = relationship
        self.variable1 = variable1
        self.variable2 = variable2
        self.relationship_name = relationship_name
        self.get_placeholder_variable()

    def get_placeholder_variable(self):
        if self.variable1 == "?g" or self.variable1 == "?h":
            self.placeholder = 0
        else:
            pass

    def __hash__(self):
        return hash(self.id_print())

    def __eq__(self, other):
        if self.id_print() == other.id_print():
            return True

        return False

    def id_print(self):
        return str(self.relationship) + "(" + self.variable1 + "," + self.variable2 + ")"

    def relationship_print(self):
        return self.relationship_name + "(" + self.variable1 + "," + self.variable2 + ")"

    def neo4j_print(self):
        res = "(" + self.variable1 + ")-[:`" + str(self.relationship) + "`]->(" + self.variable2 + ")"
        res = res.replace("?", "")
        return res

    def get_variables(self):
        return self.variable1, self.variable2

