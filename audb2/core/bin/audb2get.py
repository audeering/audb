import fire

import audb2


def get(
    name: str,
    table: str,
    *,
    columns: str = None,
    **kwargs,
) -> str:  # pragma: no cover
    r"""Get table data.

    Loads a database from Artifactory and returns the content of the
    requested table. Use the additional keyword arguments to specify
    the flavor of the database, run 'audb load --help' for a list of
    available options.

    Args:
        name: name of the database
        table: name of table
        columns: name of columns to select (separated by ',').
            If 'None` returns all columns
        **kwargs: additional arguments passed to :meth:`audb.load`

    Returns:
        table data in CSV format

    """
    if 'verbose' not in kwargs:
        kwargs['verbose'] = False
    db = audb2.load(name, **kwargs)
    df = db[table].get()
    if columns is not None:
        columns = columns.split(',')
        df = df[columns]
    return df.to_csv()


def cli():
    fire.Fire(get)  # pragma: no cover


if __name__ == '__main__':
    cli()  # pragma: no cover
