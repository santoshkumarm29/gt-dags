from joblib import Parallel, delayed
import math
import numpy as np
import pandas as pd


def bootstrap_sample(data, statistic=np.mean, n_samples=2000):
    """
    Estimate the distribution of the `statistic` over the `data` by the bootstrap method.
    This randomly selects with replacement from the `data` `n_samples` times and applies
    the `statistic` each time.

    Args:
        data:      List or array of data to sample.
        statistic: Callable to apply to the sampled `data`.
        n_samples: The number of times to perform the sampling and statistic.

    Returns:
        A series of length `n_samples` where each value is the `statistic` applied to
        the sample of `data`.
    """
    np.random.seed(None)  # required for joblib processes not to use the same random state
    n_data = len(data)
    samples = np.random.choice(data, size=(n_samples, n_data), replace=True)
    bootstrap_stats = statistic(samples, axis=1)
    return pd.Series(bootstrap_stats)


def bootstrap_sample_iterate(data, statistic=np.mean, n_samples=2000, n_jobs=1):
    """
    Compute the bootstrap by iterating over batches.
    
    """
    n_data = len(data)
    max_size = max(n_data, 10 ** 7)  # arbitrary limit of number of cells for this 2-D array
    n_batch_samples = max_size // n_data
    n_batches = int(math.ceil(n_samples / n_batch_samples))
    batch_sizes = get_batch_sizes(n_samples, n_batches)
    series_list = []
    for batch_size in batch_sizes:
        series_list.append(bootstrap_sample(data, statistic=statistic, n_samples=batch_size))
    return pd.concat(series_list, ignore_index=True)


def bootstrap_sample_joblib(data, statistic=np.mean, n_samples=2000, n_jobs=-1):
    """Return the bootstrap statistic using joblib to distribute the work among cores.

    This determines an optimal value for `n_samples` to pass to the method doing the work.
    Break the `n_data` by `n_samples` array into smaller chunks, each `n_data` by `n_samples_chunk`.
    Determine n_samples_chunk by the largest array size we want to accommodate, max_size.
    """
    n_data = len(data)
    max_size = max(n_data, 10 ** 7)  # arbitrary limit of number of cells for this 2-D array
    n_batch_samples = max_size // n_data
    n_batches = int(math.ceil(n_samples / n_batch_samples))
    batch_sizes = get_batch_sizes(n_samples, n_batches)
    # print('Iterating through {} batches of {} samples each'.format(n_batches, n_batch_samples))
    series_list = Parallel(n_jobs=n_jobs)(delayed(bootstrap_sample)(
        data,
        n_samples=batch_size,
        statistic=statistic
    ) for batch_size in batch_sizes)
    return pd.concat(series_list, ignore_index=True)


def get_batch_sizes(n_samples, n_batches):
    """Utility function for returning a list of batches of length `n_batches` the sums to `num_samples`.

    """
    step = np.ceil(n_samples / n_batches).astype(np.intp)
    chunks = []
    for i in range(n_batches):
        if (i + 1) * step > n_samples:
            chunks.append(step - ((i + 1) * step - n_samples))
        else:
            chunks.append(step)
    assert sum(chunks) == n_samples
    return chunks


class VectorStats:
    """
        the lightweight class to get summary stats on a given vector
    """

    def __init__(self):
        self.__vector_real = []
        self.__sum = 0.0
        self.__count = 0
        self.__mean = 0.0
        self.__min = 0.0
        self.__max = 0.0
        self.__p25 = 0.0
        self.__median = None
        self.__p75 = 0.0
        self.__vector_sqr_error = []
        self.__mse = 0.0
        self.__stddev = 0.0
        self.__lhs_half = []
        self.__rhs_half = []
        self.__quartile_calc = False

    def __quartile(self):
        """
        Support p25, p50 and p75
        :return:
        """
        self.__quartile_calc = True
        self.__vector_real.sort()
        if self.__count >= 2:
            p50 = self.__count // 2
            if self.__count % 2 == 0:
                self.__median = (self.__vector_real[p50 - 1] + self.__vector_real[p50]) / 2.0
                self.__lhs_half = self.__vector_real[0:p50]
                self.__rhs_half = self.__vector_real[p50:]
            else:
                self.__median = self.__vector_real[p50]
                self.__lhs_half = self.__vector_real[0:p50]
                self.__rhs_half = self.__vector_real[p50 + 1:]
            lhs = len(self.__lhs_half)
            if lhs >= 2:
                p25 = lhs // 2
                if lhs % 2 == 0:
                    self.__p25 = (self.__lhs_half[p25 - 1] + self.__lhs_half[p25]) / 2.0
                else:
                    self.__p25 = self.__lhs_half[p25]
            rhs = len(self.__rhs_half)
            if rhs >= 2:
                p75 = rhs // 2
                if rhs % 2 == 0:
                    self.__p75 = (self.__rhs_half[p75 - 1] + self.__rhs_half[p75]) / 2.0
                else:
                    self.__p75 = self.__rhs_half[p75]
            self.__lhs_half = []
            self.__rhs_half = []

    def append(self, real_number=0.0):

        self.__quartile_calc = False

        self.__vector_real.append(real_number)

        self.__sum += real_number

        self.__count += 1

        self.__mean = self.__sum / self.__count

        if self.__count == 1:
            self.__min = real_number
            self.__p25 = real_number
            self.__median = real_number
            self.__p75 = real_number
            self.__max = real_number

        if real_number < self.__min:
            self.__min = real_number

        if real_number > self.__max:
            self.__max = real_number

    def sum(self):
        return self.__sum

    def count(self):
        return self.__count

    def mean(self):
        return self.__mean

    def min(self):
        return self.__min

    def max(self):
        return self.__max

    def p25(self):
        """
        its only necessary to execute __quartile() after append()
        __quartile_calc boolean helps with that
        :return:
        """
        if not self.__quartile_calc:
            self.__quartile()
        return self.__p25

    def median(self):
        """
        its only necessary to execute __quartile() after append()
        __quartile_calc boolean helps with that
        :return:
        """
        if not self.__quartile_calc:
            self.__quartile()
        return self.__median

    def p75(self):
        """
        its only necessary to execute __quartile() after append()
        __quartile_calc boolean helps with that
        :return:
        """
        if not self.__quartile_calc:
            self.__quartile()
        return self.__p75

    def stddev(self):
        """
        Sample Standard Deviation
        :return:
        """

        if self.__count > 2:
            self.__vector_real.sort()
            self.__vector_sqr_error = [(r - self.__mean) ** 2 for r in self.__vector_real]
            self.__mse = sum(self.__vector_sqr_error) / (self.__count - 1)
            import math
            self.__stddev = math.sqrt(self.__mse)
            self.__vector_sqr_error = []

        return self.__stddev


def check_vectorstats_return_true():

    agg = VectorStats()
    agg.append(1)
    agg.append(3)
    agg.append(-1000)
    agg.append(6)
    agg.append(-9)

    agg1 = VectorStats()
    agg1.append(2)
    agg1.append(1.9)
    agg1.append(2.1)
    agg1.append(1.98)
    agg1.append(2.01)

    assert True == (
        round(agg.sum(),4) == -999.0 and
        agg.count() == 5 and
        round(agg.mean(),4) == -199.8 and
        round(agg.min(),4) == -1000 and
        round(agg.p25(),4) == -504.5 and
        round(agg.median(),4) == 1 and
        round(agg.p75(), 4) == 4.5 and
        round(agg.max(), 4) == 6 and
        round(agg.stddev(), 4) == 447.3608
    )

    agg.append(7)

    assert True == (
        round(agg.sum(),4) == -992.0 and
        agg.count() == 6 and
        round(agg.mean(),4) == -165.3333 and
        round(agg.min(),4) == -1000 and
        round(agg.p25(),4) == -9 and
        round(agg.median(),4) == 2.0 and
        round(agg.p75(), 4) == 6 and
        round(agg.max(), 4) == 7 and
        round(agg.stddev(), 4) == 408.9414
    )

    agg.append(7)

    assert True == (
        round(agg.sum(),4) == -985.0 and
        agg.count() == 7 and
        round(agg.mean(),4) == -140.7143 and
        round(agg.min(),4) == -1000 and
        round(agg.p25(),4) == -9 and
        round(agg.median(),4) == 3 and
        round(agg.p75(), 4) == 7 and
        round(agg.max(), 4) == 7 and
        round(agg.stddev(), 4) == 378.9506
    )

    agg.append(8)

    assert True == (
        round(agg.sum(),4) == -977.0 and
        agg.count() == 8 and
        round(agg.mean(),4) == -122.125 and
        round(agg.min(),4) == -1000 and
        round(agg.p25(),4) == -4.0 and
        round(agg.median(),4) == 4.5 and
        round(agg.p75(), 4) == 7.0 and
        round(agg.max(), 4) == 8 and
        round(agg.stddev(), 4) == 354.758
    )

    assert True == (
        round(agg1.sum(),4) == 9.99 and
        agg1.count() == 5 and
        round(agg1.mean(),4) == 1.998 and
        round(agg1.min(),4) == 1.9 and
        round(agg1.p25(),4) == 1.94 and
        round(agg1.median(),4) == 2 and
        round(agg1.p75(), 4) == 2.055 and
        round(agg1.max(), 4) == 2.1 and
        round(agg1.stddev(), 4) == 0.0716
    )
