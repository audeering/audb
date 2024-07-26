import pandas as pd
import tabulate

import audeer


cache = audeer.mkdir("./cache")
engines = ["", "_polars"]


def parse_df(fp, engine):
    df = pd.read_csv(fp, index_col="method")
    df["engine"] = engine
    if engine == "pandas":
        df.drop(columns=["string", "object"], inplace=True)

    df.rename(columns={"polars": "t", "pyarrow": "t"}, inplace=True)
    return df


def get_col(sr):
    """Return columns to annotate df."""
    name = sr.idxmin()
    return [name, sr.max() / sr.min()]


df_polars = parse_df(audeer.path(cache, "results_polars.csv"), "polars")
df_pandas = parse_df(audeer.path(cache, "results.csv"), "pandas")
df = pd.concat([df_pandas, df_polars], axis=0)

df = df.reset_index().pivot(
    index="method",
    columns="engine",
    values="t",
)


df[["winner", "factor"]] = df[["pandas", "polars"]].apply(
    lambda x: get_col(x), axis=1, result_type="expand"
)


table = tabulate.tabulate(df, headers="keys", tablefmt="github", floatfmt=".3f")
print(table)
