import inspect

import fire

import audb2


def cli():  # pragma: no cover
    functions = {
        name: func for name, func in inspect.getmembers(
            audb2.info, inspect.isfunction
        )
    }
    fire.Fire(functions)


if __name__ == '__main__':
    cli()  # pragma: no cover
