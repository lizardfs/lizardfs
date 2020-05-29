import argparse
from functools import lru_cache
from itertools import combinations
from typing import Any, Iterable, List, Tuple
from random import sample


@lru_cache(maxsize=None)
def binomial(n: int, k: int) -> int:
    assert k >= 0 and n >= 0 and k <= n

    if k == 0 or k == n:
        return 1
    elif k == 1 or k == n - 1:
        return n
    elif 2 * k > n:
        return binomial(n, n - k)
    else:
        return binomial(n - 1, k - 1) + binomial(n - 1, k)


def ith_combination_of_fixed_size(
    n: int, k: int, idx: int, offset: int = 1, result: List[int] = []
) -> List[int]:
    upper_bound = binomial(n, k)

    if k == 0:
        return result

    assert idx <= upper_bound and idx >= 1

    s1 = binomial(n - 1, k - 1)
    if idx <= s1:
        return ith_combination_of_fixed_size(
            n - 1, k - 1, idx, offset + 1, result + [offset]
        )
    else:
        return ith_combination_of_fixed_size(n - 1, k, idx - s1, offset + 1, result)


def random_subsets_of_fixed_size(
    n: int, k: int, count: int
) -> Tuple[List[int], List[List[int]]]:
    upper_bound = binomial(n, k)
    idx = sorted(list(sample(range(1, upper_bound + 1), count)))
    return (
        idx,
        list(map(lambda i: ith_combination_of_fixed_size(n, k, i), idx)),
    )


def all_combinations(n: int, k: int) -> Iterable[List[int]]:
    return list(map(lambda x: list(x), combinations(range(1, n + 1), k)))


def selected_combinations(n: int, k: int) -> Iterable[List[int]]:
    for i in range(1, min(k, n) + 1):
        for s in range(0, i + 1):
            yield sorted(sample(range(1, n + 1), s)) + sorted(
                sample(range(n + 1, n + k + 1), i - s)
            )


def bashprint(x: Iterable[Any], dim: int = 1) -> None:
    """
    Prints lists in a bash-readable format.
    """
    if dim == 1:
        s = str(x)
        for c in " []()":
            s = s.replace(c, "")
        print(s)
    elif dim == 2:
        for i in x:
            bashprint(i, 1)
    else:
        raise Exception("unimplemented")


def print_random_subsets_of_fixed_size(
    n: int, k: int, count: int, porcelain: bool = True
) -> None:
    """
    Prints random subsets in the following format
        i1,i2,i3,... # indexes of combinations
        x11,x12,...,x1k
        ...
        xi1,...,xik # ith combination
    """
    idx, res = random_subsets_of_fixed_size(n, k, count)

    if porcelain:
        bashprint(idx)
        bashprint(res, 2)
    else:
        print(idx)
        print(res)


def print_all_combinations(n: int, k: int, porcelain: bool = True) -> None:
    res = all_combinations(n, k)
    if porcelain:
        bashprint(res, 2)
    else:
        print(res)


def print_selected_combinations(n: int, k: int, porcelain: bool = True) -> None:
    res = list(selected_combinations(n, k))
    if porcelain:
        bashprint(res, 2)
    else:
        print(res)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Process combinations")
    parser.add_argument(
        "--porcelain", action="store_true", help="Bash friendly printing"
    )
    parser.add_argument(
        "--random_subsets_of_fixed_size", nargs=3, metavar=("n", "k", "count"), type=int
    )
    parser.add_argument("--all_combinations", nargs=2, metavar=("n", "k"), type=int)
    parser.add_argument(
        "--selected_combinations", nargs=2, metavar=("n", "k"), type=int
    )
    args = parser.parse_args()

    parameters = args.all_combinations
    if parameters:
        print_all_combinations(*parameters)

    parameters = args.random_subsets_of_fixed_size
    if parameters:
        print_random_subsets_of_fixed_size(*parameters)

    parameters = args.selected_combinations
    if parameters:
        print_selected_combinations(*parameters)
