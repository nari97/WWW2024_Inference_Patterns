import math
import pickle
import torch
import numpy as np

def frac_rank(less, eq):
    return (2 * less + eq + 1) / 2

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
        collector_base = RankCollector()
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

                scoreCHeads = triples[1:corruptedHeadsEnd, 3]
                scoreCTails = triples[corruptedHeadsEnd:, 3]
                positive_triple_score = triples[0, 3]

                rankhLess, ranktLess = np.sum(positive_triple_score < scoreCHeads).item(), np.sum(
                    positive_triple_score < scoreCTails).item()
                rankhEq, ranktEq = 1 + np.sum(positive_triple_score == scoreCHeads).item(), 1 + np.sum(
                    positive_triple_score == scoreCTails).item()

                collector_base.update_rank(frac_rank(rankhLess, rankhEq), rankhEq > 1, len(corruptedHeads),
                                           frac_rank(ranktLess, ranktEq), ranktEq > 1, len(corruptedTails), t.r,
                                           self.manager.relation_anomaly[t.r])

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
        return collector_base

    def predict(self, arrH, arrR, arrT, model):
        def to_var(x):
            return torch.LongTensor(x)

        return model.predict({
            'batch_h': to_var(arrH),
            'batch_r': to_var(arrR),
            'batch_t': to_var(arrT),
            'mode': 'normal'
        })

class RankCollector:
    def __init__(self):
        self.all_ranks = []
        self.all_totals = []
        self.all_rels = []
        self.all_anomalies = []
        self.all_ties = []
        self.unique_triples_materialized = {}
        self.total_unique_triples = {}

    def load(self, r, t):
        self.all_ranks = r
        self.all_totals = t

    def prune(self, max_anom, min_anom):
        rc = RankCollector()
        for i in range(len(self.all_anomalies)):
            if self.all_anomalies[i] < min_anom or self.all_anomalies[i] > max_anom:
                continue

            rc.all_ranks.append(self.all_ranks[i])
            rc.all_totals.append(self.all_totals[i])
            rc.all_rels.append(self.all_rels[i])
            rc.all_anomalies.append(self.all_anomalies[i])
            rc.all_ties.append(self.all_ties[i])

            if self.all_rels[i] in self.unique_triples_materialized.keys() \
                    and self.all_rels[i] not in rc.unique_triples_materialized.keys():
                rc.unique_triples_materialized[self.all_rels[i]] = self.unique_triples_materialized[self.all_rels[i]]
            if self.all_rels[i] in self.total_unique_triples.keys() \
                    and self.all_rels[i] not in rc.total_unique_triples.keys():
                rc.total_unique_triples[self.all_rels[i]] = self.total_unique_triples[self.all_rels[i]]
        return rc

    # Checks whether we should stop training.
    def stop_train(self, previous, metric_str="mr"):
        if previous is None:
            return False

        current_metric = self.get_metric(metric_str=metric_str)

        # If the current metric is improved by previous and it is significant.
        if Metric.is_improved(current_metric, previous.get_metric(metric_str=metric_str)) and \
                RankCollector.is_significant(self.all_ranks, previous.all_ranks):
            return True

        # If the current metric is improved by random and it is significant.
        if Metric.is_improved(current_metric, previous.get_expected(metric_str=metric_str)) and \
                self.is_significant_expected():
            return True

        return False

    def update_rank(self, rankh, hHasTies, totalh, rankt, tHasTies, totalt, r, anomaly):
        self.all_ranks.append(rankh)
        self.all_ties.append(hHasTies)
        self.all_ranks.append(rankt)
        self.all_ties.append(tHasTies)
        self.all_totals.append(totalh)
        self.all_totals.append(totalt)
        self.all_rels.append(r)
        self.all_rels.append(r)
        self.all_anomalies.append(anomaly)
        self.all_anomalies.append(anomaly)

    # TODO This method and 'get_expected' are doing the same.
    def get_ranks_below_expected(self):
        below = []
        for i in range(len(self.all_totals)):
            below.append(self.all_ranks[i] > (self.all_totals[i] + 1) / 2)
        return below

    def update_unique_materialized(self, r):
        if r not in self.unique_triples_materialized.keys():
            self.unique_triples_materialized[r] = 0
        self.unique_triples_materialized[r] = self.unique_triples_materialized[r] + 1

    def update_total_unique_triples(self, r):
        if r not in self.total_unique_triples.keys():
            self.total_unique_triples[r] = 0
        self.total_unique_triples[r] = self.total_unique_triples[r] + 1

    def get_expected(self, metric_str="mr"):
        expected = []
        for i in range(len(self.all_totals)):
            expected.append((self.all_totals[i] + 1) / 2)
        return self.get(expected, self.all_totals, metric_str)

    def get_metric(self, metric_str="mr"):
        return self.get(self.all_ranks, self.all_totals, metric_str)

    @staticmethod
    def is_significant(these_ranks, other_ranks, threshold=.05):
        pass

    def is_significant_expected(self):
        expected = []
        for i in range(len(self.all_totals)):
            expected.append((self.all_totals[i] + 1) / 2)
        return RankCollector.is_significant(self.all_ranks, expected)

    # TODO Compute these using mean and include variance.
    def get(self, ranks, totals, metric_str):
        if len(ranks) == 0:
            return Metric(0)
        cmp = 'low'
        if metric_str == 'mr':
            value = np.sum(ranks) / len(totals)
        elif metric_str == 'amr':
            value = 1.0 - (self.get(ranks, totals, 'mr').get() / self.get_expected(metric_str='mr').get())
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
        if metric_str == 'mrr':
            # TODO Can this be done using Numpy?
            value = 0
            for i in range(len(ranks)):
                value += 1/ranks[i]
            value = value / len(totals)
            cmp = 'high'
        return Metric(value, cmp)

class Metric():
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