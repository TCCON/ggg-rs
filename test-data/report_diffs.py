#!/usr/bin/env python3
from argparse import ArgumentParser
import pandas as pd

def main():
    p = ArgumentParser(description='Report differences in the values of GGG output text files')
    p.add_argument('expected_file', help='File with the expected values')
    p.add_argument('new_file', help='File produced by the tests')
    clargs = p.parse_args()

    df_expected = read_file(clargs.expected_file)
    df_new = read_file(clargs.new_file)

    for colname, colvals in df_expected.items():
        for row, rowval in colvals.items():
            newval = df_new.loc[row, colname]
            if newval != rowval:
                print(f'Column {colname}, row {row+1} values differ: {rowval} expected, got {newval}')


def read_file(path):
    with open(path) as f:
        line = f.readline()
        nhead = int(line.split()[0])
        for _ in range(nhead-1):
            line = f.readline()
        colnames = line.split()
        values = []
        for line in f:
            values.append(line.split())
        return pd.DataFrame(values, columns=colnames)


if __name__ == '__main__':
    main()
