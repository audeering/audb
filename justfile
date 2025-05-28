# build documentation
docs:
  uv run python -m sphinx docs/ build/html -b html

# run tests
test *ARGS:
  uv run pytest {{ARGS}}

# run pre-commit
pre-commit:
  uvx pre-commit install
  uvx pre-commit run --all-files
