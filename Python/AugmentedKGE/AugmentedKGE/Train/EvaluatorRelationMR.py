import heapq
import pickle
import torch
import numpy as np
import math


# import time

def get_expected(all_totals, metric_str="mr"):
    expected = []
    for i in range(len(all_totals)):
        expected.append((all_totals[i] + 1) / 2)
    return get(expected, all_totals, metric_str)


def get(ranks, totals, metric_str):
    if len(ranks) == 0:
        return Metric(0)
    cmp = 'low'
    if metric_str == 'mr':
        value = np.sum(ranks) / len(totals)
    elif metric_str == 'amr':
        value = 1.0 - (get(ranks, totals, 'mr').get() / get_expected(totals, metric_str='mr').get())
        cmp = 'high'
    elif metric_str == 'wmr':
        # TODO Can this be done using Numpy?
        value, divisor = 0, 0
        for i in range(len(ranks)):
            value += totals[i] * ranks[i]
            divisor += totals[i]
        value = value / divisor
    elif metric_str == 'gmr':
        a = np.log(ranks)
        value = np.exp(a.sum() / len(a))
    elif metric_str == 'wgmr':
        # TODO Can this be done using Numpy?
        value, divisor = 0, 0
        for i in range(len(ranks)):
            value += totals[i] * math.log(ranks[i])
            divisor += totals[i]
        value = math.exp(value / divisor)
    elif metric_str == 'matsize':
        value = np.sum(ranks)
    elif metric_str == 'mrr':
        # TODO Can this be done using Numpy?
        value = 0
        for i in range(len(ranks)):
            value += 1 / ranks[i]
        value = value / len(totals)
        cmp = 'high'

    return Metric(value, cmp)


class EvaluatorRelationMR(object):
    """ This can be either the validator or tester depending on the manager used. E"""

    def __init__(self, manager=None, rel_anomaly_max=.75, rel_anomaly_min=0, batched=False):
        self.manager = manager
        self.rel_anomaly_max = rel_anomaly_max
        self.rel_anomaly_min = rel_anomaly_min
        self.batched = batched

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

    def evaluate(self, model, materialize=False, materialize_basefile=None):

        is_nan_cnt, total = 0, 0
        # We will split the evaluation by relation
        relations = {}
        triple_stats_across_relations = {}
        n_positives_ranked_before_expected = {}
        ctr = 0
        relation_mrs = {}
        for t in self.manager.get_triples():
            # We will not consider these anomalies.
            if self.manager.relation_anomaly[t.r] < self.rel_anomaly_min or \
                    self.manager.relation_anomaly[t.r] > self.rel_anomaly_max:
                continue
            if t.r not in relations.keys():
                relations[t.r] = []
            relations[t.r].append(t)

        for r in relations.keys():
            print(f"Relation: {r}, Relations left: {len(relations) - ctr}")
            mrs = []
            totals = []
            n_positives_ranked_before_expected[r] = 0
            total += 1
            # start = time.perf_counter()
            # print("Relation", r, ":", total, "out of", len(relations))
            if materialize:
                triple_stats = {}
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

                if not self.batched:
                    scores = self.predict(triples[:, 0], triples[:, 1], triples[:, 2], model)
                else:
                    batch_size = 25
                    arrH_batches = np.array_split(triples[:, 0], batch_size)
                    arrR_batches = np.array_split(triples[:, 1], batch_size)
                    arrT_batches = np.array_split(triples[:, 2], batch_size)
                    scores = None

                    for i in range(len(arrT_batches)):
                        batch_score = self.predict(arrH_batches[i], arrR_batches[i], arrT_batches[i], model)
                        if scores is None:
                            scores = batch_score
                        else:
                            scores = torch.concat((scores, batch_score))

                # Adding scores to the triples
                triples[:, 3] = scores.detach().numpy()
                cHeads = triples[1:corruptedHeadsEnd, 3]
                cTails = triples[corruptedHeadsEnd:, 3]
                positive_triple_score = triples[0, 3]

                # Changed PyTorch.sum to numpy.sum as positive_triple_score, cHeads, cTails are all numpy arrays now.
                # We are expecting positives to have higher scores than negatives, so we need to add to the rank when
                #   the negatives scores are higher.
                rankhLess, ranktLess = np.sum(positive_triple_score < cHeads).item(), np.sum(
                    positive_triple_score < cTails).item()
                rankhEq, ranktEq = 1 + np.sum(positive_triple_score == cHeads).item(), 1 + np.sum(
                    positive_triple_score == cTails).item()

                # If it is NaN, rank last!
                if np.isnan(positive_triple_score.item()):
                    is_nan_cnt += 1
                    rankhLess, ranktLess = len(cHeads), len(cTails)
                    rankhEq, ranktEq = 1, 1

                # Compute ranks and expected rank
                # TODO fractional ranks are computed again below.
                # TODO Expected rank is computed in the collector.
                rankH = self.frac_rank(rankhLess, rankhEq)
                rankT = self.frac_rank(ranktLess, ranktEq)

                mrs.append(rankH)
                mrs.append(rankT)
                totals.append(len(cHeads))
                totals.append(len(cTails))

            relation_mrs[r] = get(mrs, totals, "amr").get()
            ctr += 1

        with open(materialize_basefile, "w+") as f:
            for relation in relation_mrs:
                f.write(f"{relation},{relation_mrs[relation]}\n")

    def add_triple(self, tree, h, r, t, i):
        if (h, t) not in tree.keys():
            tree[(h, t)] = np.array((0, 0))
        tree[(h, t)][i] = tree[(h, t)][i] + 1

    def frac_rank(self, less, eq):
        return (2 * less + eq + 1) / 2

    def predict(self, arrH, arrR, arrT, model):
        def to_var(x):
            return torch.LongTensor(x)

        return model.predict({
            'batch_h': to_var(arrH),
            'batch_r': to_var(arrR),
            'batch_t': to_var(arrT),
            'mode': 'normal'
        })


class Metric:
    def __init__(self, value, cmp='low'):
        self.value = value
        self.cmp = cmp

    # Check whether this is improved by that.
    @staticmethod
    def is_improved(this, that):
        if this.cmp != that.cmp:
            raise ValueError('Comparison types of this (' + this.cmp + ') and that (' + that.cmp + ') are different')
        if this.cmp == 'low':
            return this.get() > that.get()
        elif this.cmp == 'high':
            return this.get() < that.get()

    def get(self):
        return self.value
