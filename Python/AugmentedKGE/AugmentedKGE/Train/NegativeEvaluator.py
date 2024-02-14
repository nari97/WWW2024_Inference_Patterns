import torch
import numpy as np
from RankCollector import RankCollector


def predict(arrH, arrR, arrT, model):
    def to_var(x):
        return torch.LongTensor(x)

    return model.predict({
        'batch_h': to_var(arrH),
        'batch_r': to_var(arrR),
        'batch_t': to_var(arrT),
        'mode': 'normal'
    })


class NegativeEvaluator(object):

    def __init__(self, manager=None, rel_anomaly_max=.75, rel_anomaly_min=0):
        self.manager = manager
        self.rel_anomaly_max = rel_anomaly_max
        self.rel_anomaly_min = rel_anomaly_min

    def evaluate(self, model, negatives):
        relations = {}
        # subject_ranks = []
        # object_ranks = []
        # subject_prime_ranks = []
        # object_prime_ranks = []
        collector_ranks = RankCollector()
        collector_ranks_prime = RankCollector()
        for t in self.manager.get_triples():
            if self.manager.relation_anomaly[t.r] < self.rel_anomaly_min or \
                    self.manager.relation_anomaly[t.r] > self.rel_anomaly_max:
                continue
            if t.r not in relations.keys():
                relations[t.r] = []
            relations[t.r].append(t)

        for r in relations.keys():
            for t in relations[r]:
                rs = 1
                rs_prime = 1
                ro = 1
                ro_prime = 1
                min_score_s = float('inf')
                min_score_o = float('inf')
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
                scores = predict(triples[:, 0], triples[:, 1], triples[:, 2], model)

                triples[:, 3] = scores.detach().numpy()
                cHeads = triples[1:corruptedHeadsEnd, 3]
                cTails = triples[corruptedHeadsEnd:, 3]
                positive_triple_score = triples[0, 3]

                for i in range(len(cHeads)):
                    negative = (cHeads[i, 0], cHeads[i, 2])
                    negative_score = cHeads[i, 3]
                    if positive_triple_score < negative_score:

                        if t.r not in negatives:
                            rs += 1
                        elif negative not in negatives[t.r]:
                            rs += 1

                    if t.r in negatives and negative in negatives and negative_score < min_score_s:
                        min_score_s = negative_score

                for i in range(len(cHeads)):
                    negative_score = cHeads[i, 3]
                    if negative_score > min_score_s:
                        rs_prime += 1

                for i in range(len(cTails)):
                    negative = (cTails[i, 0], cTails[i, 2])
                    negative_score = cTails[i, 3]
                    if positive_triple_score < negative_score:
                        if t.r not in negatives:
                            ro += 1
                        elif negative not in negatives[t.r]:
                            ro += 1

                    if t.r in negatives and negative in negatives and negative_score < min_score_o:
                        min_score_o = negative_score

                for i in range(len(cTails)):
                    negative_score = cTails[i, 3]
                    if negative_score > min_score_o:
                        ro_prime += 1

                collector_ranks.update_rank(rs, rs > 1, len(corruptedHeads),
                                            ro, ro > 1, len(corruptedTails), t.r,
                                            self.manager.relation_anomaly[t.r])
                collector_ranks_prime.update_rank(rs_prime, rs_prime > 1, len(corruptedHeads),
                                                  ro_prime, ro_prime > 1, len(corruptedTails), t.r,
                                                  self.manager.relation_anomaly[t.r])

        return collector_ranks, collector_ranks_prime
