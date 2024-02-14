import torch
import numpy as np
import math
from scipy.stats import wilcoxon

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
        return wilcoxon(these_ranks, other_ranks, zero_method='pratt').pvalue < threshold

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