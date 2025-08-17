devenv:
	poetry config virtualenvs.in-project true && poetry install

summarize:
	poetry run python -m main.py

unblocksa:
	poetry run python -m sa.py