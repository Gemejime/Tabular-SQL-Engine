.PHONY: format lint typecheck security test-unit test-security test-chaos test check

PYTHON ?= python

format:
	$(PYTHON) -m ruff format .

lint:
	$(PYTHON) -m ruff check .
	$(PYTHON) -m ruff format --check .

typecheck:
	$(PYTHON) -m mypy internal

security:
	$(PYTHON) -m bandit -r internal -ll
	$(PYTHON) -m pip_audit

test-unit:
	$(PYTHON) -m pytest -v test/unit

test-security:
	$(PYTHON) -m pytest -v test/security

test-chaos:
	$(PYTHON) -m pytest -v test/chaos

test:
	$(PYTHON) -m pytest -v test/unit test/security test/chaos

check: lint typecheck security test
