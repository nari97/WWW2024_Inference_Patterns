import pickle
import torch
import numpy as np


class RankMaterializer(object):
    """

    """

    def __init__(self, manager=None, rel_anomaly_max=.75, rel_anomaly_min=0, batched=False, triple_to_id=None):
        self.manager = manager
        self.rel_anomaly_max = rel_anomaly_max
        self.rel_anomaly_min = rel_anomaly_min
        self.batched = batched
        self.triple_to_id = triple_to_id

    def get_totals(self):
        totals = []
        # We will split the evaluation by relation
        relations = {}
        for t in self.manager.get_triples():
            # We will not consider these anomalies.
            if self.manager.relation_anomaly[t.r] < self.rel_anomaly_min or \
                    self.manager.relation_anomaly[t.r] > self.rel_anomaly_max:
                continue
            if t.r not in relations.keys():
                relations[t.r] = []
            relations[t.r].append(t)

        for r in relations.keys():
            for t in relations[r]:
                totals.append(len(self.manager.get_corrupted(t.h, t.r, t.t, "head")))
                totals.append(len(self.manager.get_corrupted(t.h, t.r, t.t, "tail")))

        return totals

    def collect_ranks(self, model, materialize_basefile: str):
        """
        This function is used to collect the ranks of the negative triples for a given link prediction model.
        Args:
            model: The LinkPredictionModel to use for scoring triples.
            materialize_basefile: String path to folder containing materialized triples.

        Returns:

        """
        is_nan_cnt, total = 0, 0
        # We will split the evaluation by relation
        relations = {}
        materialized_triples = {}
        positive_triple_counter = 0
        for t in self.manager.get_triples():
            # We will not consider these anomalies.
            if self.manager.relation_anomaly[t.r] < self.rel_anomaly_min or \
                    self.manager.relation_anomaly[t.r] > self.rel_anomaly_max:
                continue
            if t.r not in relations.keys():
                relations[t.r] = []
            relations[t.r].append(t)

        for r in relations.keys():
            print(f"Processing relation: {total}/{len(relations)}: {r}")
            total += 1
            for t in relations[r]:
                corruptedHeads = self.manager.get_corrupted(t.h, t.r, t.t, "head")
                corruptedTails = self.manager.get_corrupted(t.h, t.r, t.t, "tail")

                totalTriples = 1 + len(corruptedHeads) + len(corruptedTails)

                triples = np.zeros((totalTriples, 4))

                triples[0, 0:3] = [t.h, t.r, t.t]

                triples[1:1 + len(corruptedHeads), 0] = list(corruptedHeads)
                triples[1:1 + len(corruptedHeads), 1] = t.r
                triples[1:1 + len(corruptedHeads), 2] = t.t

                corruptedHeadsEnd = 1 + len(corruptedHeads)

                triples[1 + len(corruptedHeads):, 0] = t.h
                triples[1 + len(corruptedHeads):, 1] = t.r
                triples[1 + len(corruptedHeads):, 2] = list(corruptedTails)

                scores = self.predict(triples[:, 0], triples[:, 1], triples[:, 2], model).flatten()
                triples[:, 3] = scores.detach().numpy()

                triples_head_corrupted = triples[1:corruptedHeadsEnd]
                triples_tail_corrupted = triples[corruptedHeadsEnd:]

                triples_head_corrupted = triples_head_corrupted[triples_head_corrupted[:, 3].argsort()[::-1]]
                triples_tail_corrupted = triples_tail_corrupted[triples_tail_corrupted[:, 3].argsort()[::-1]]

                # triples_tail_corrupted.sort(key=lambda x: x[3], reverse=True)
                # triples_head_corrupted.sort(key=lambda x: x[3], reverse=True)

                rank_positive_head = 1
                rank_positive_tail = 1
                positive_triples = (triples[0][0], triples[0][1], triples[0][2])

                for cur_triple in triples_head_corrupted:
                    if cur_triple[3] >= triples[0][3]:
                        rank_positive_head += 1

                for cur_triple in triples_tail_corrupted:
                    if cur_triple[3] >= triples[0][3]:
                        rank_positive_tail += 1

                ctr = 0
                for index in range(len(triples_head_corrupted)):
                    triple = triples_head_corrupted[index]
                    if triple[3] >= triples[0][3] and ctr < 100:
                        triple_to_add = (int(triple[0]), int(triple[1]), int(triple[2]))

                        if triple_to_add not in materialized_triples:
                            materialized_triples[triple_to_add] = [[], [], [], []]

                        materialized_triples[triple_to_add][0].append([index + 1, triple[3]])
                        materialized_triples[triple_to_add][2].append(
                            [self.triple_to_id[(triples[0][0], triples[0][1], triples[0][2])], rank_positive_head,
                             triples[0][3]])
                        ctr += 1

                if ctr < 100:
                    if positive_triples not in materialized_triples:
                        materialized_triples[positive_triples] = [[], [], [], []]
                        positive_triple_counter += 1

                    materialized_triples[positive_triples][2].append(
                        [self.triple_to_id[positive_triples], rank_positive_head,
                         triples[0][3]])


                ctr = 0
                for index in range(len(triples_tail_corrupted)):
                    triple = triples_tail_corrupted[index]
                    if triple[3] >= triples[0][3] and ctr < 100:
                        triple_to_add = (int(triple[0]), int(triple[1]), int(triple[2]))

                        if triple_to_add not in materialized_triples:
                            materialized_triples[triple_to_add] = [[], [], [], []]

                        materialized_triples[triple_to_add][1].append([index + 1, triple[3]])
                        materialized_triples[triple_to_add][3].append(
                            [self.triple_to_id[(triples[0][0], triples[0][1], triples[0][2])], rank_positive_tail,
                             triples[0][3]])
                        ctr += 1

                if ctr < 100:
                    if positive_triples not in materialized_triples:
                        materialized_triples[positive_triples] = [[], [], [], []]
                        positive_triple_counter += 1

                    materialized_triples[positive_triples][3].append(
                        [self.triple_to_id[positive_triples], rank_positive_tail,
                         triples[0][3]])


        print(f"Number of positive triples: {positive_triple_counter}")
        print(f"Number of negative triples: {len(materialized_triples) - positive_triple_counter}")
        top_k_file = open(f"{materialize_basefile}_inference.pickle", "wb")
        pickle.dump(materialized_triples, top_k_file)
        top_k_file.close()

    def predict(self, arrH, arrR, arrT, model):
        def to_var(x):
            return torch.LongTensor(x)

        return model.predict({
            'batch_h': to_var(arrH),
            'batch_r': to_var(arrR),
            'batch_t': to_var(arrT),
            'mode': 'normal'
        })
