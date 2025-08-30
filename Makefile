devenv:
	poetry config virtualenvs.in-project true && poetry install --noroot && poetry run playwright install --with-deps chromium

summarize:
	poetry run python -m main.py

unblocksa:
	poetry run python -m sa.py $(url)