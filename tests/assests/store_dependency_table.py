import argparse

import audb


def main(outfile):
    """Load dependency and store at requested file."""
    deps = audb.Dependencies()
    deps.load("db.csv")
    deps.save(outfile)


if __name__ == "__main__":
    # Call the program with:
    #
    # $ python store_dependency_table.py emodb-audb-1.6.4.pkl
    #
    parser = argparse.ArgumentParser()
    parser.add_argument("outfile")
    args = parser.parse_args()
    main(args.outfile)
