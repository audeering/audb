import argparse

import audb


def main(pandas_version):
    """Load dependency from CSV and store as PKL file.

    Args:
        pandas_version: version of installed ``pandas`` package

    """
    # Download emodb dependency table
    # from version 1.4.1,
    # which is still stored as CSV file
    repository = audb.Repository(
        "data-public",
        "https://audeering.jfrog.io/artifactory",
        "artifactory",
    )
    backend_interface = repository.create_backend_interface()
    remote_file = backend_interface.join("/", "emodb", "db.zip")
    with backend_interface.backend:
        backend_interface.get_archive(remote_file, ".", "1.4.1", verbose=False)

    deps = audb.Dependencies()
    deps.load("db.csv")
    outfile = f"emodb-pandas-{pandas_version}.pkl"
    deps.save(outfile)


if __name__ == "__main__":
    # Call the program with:
    #
    # $ python store_dependency_table.py 2.2.2
    #
    # where 2.2.2 refers to the installed pandas version.
    parser = argparse.ArgumentParser()
    parser.add_argument("pandas_version")
    args = parser.parse_args()
    main(args.pandas_version)
