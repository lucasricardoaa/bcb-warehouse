.PHONY: lint typecheck test-glue dbt-parse dbt-compile dbt-run dbt-test dbt-seed install install-dev

DBT_PROJECT_DIR = dbt/bcb_warehouse
# dbt roda via venv Python 3.12 (dbt-core 1.11 incompatível com Python 3.14 do sistema)
DBT = .venv312/Scripts/dbt

# ── Qualidade de código ───────────────────────────────────────────────────────

lint:
	ruff check glue/ dags/ tools/ tests/

typecheck:
	mypy glue/ dags/ tools/

# ── Testes unitários do Glue Job ──────────────────────────────────────────────

test-glue:
	pytest tests/unit/glue/ -v

# ── dbt ───────────────────────────────────────────────────────────────────────

dbt-parse:
	# Valida sintaxe YAML/SQL sem conexão com AWS. Usar para dev local.
	$(DBT) parse --project-dir $(DBT_PROJECT_DIR) --profiles-dir $(DBT_PROJECT_DIR)

dbt-compile:
	# Requer AWS credentials (Athena adapter conecta no Glue para popular cache).
	$(DBT) compile --project-dir $(DBT_PROJECT_DIR) --profiles-dir $(DBT_PROJECT_DIR)

dbt-seed:
	$(DBT) seed --project-dir $(DBT_PROJECT_DIR) --profiles-dir $(DBT_PROJECT_DIR)

dbt-run:
	$(DBT) run --project-dir $(DBT_PROJECT_DIR) --profiles-dir $(DBT_PROJECT_DIR)

dbt-test:
	$(DBT) test --project-dir $(DBT_PROJECT_DIR) --profiles-dir $(DBT_PROJECT_DIR)

# ── Instalação ────────────────────────────────────────────────────────────────

install:
	pip install -e .

install-dev:
	pip install -e ".[dev]"

install-dbt:
	# Cria venv Python 3.12 com dbt-core + adapter Athena
	py -3.12 -m venv .venv312
	.venv312/Scripts/pip install --quiet dbt-core==1.11.7 dbt-athena-community
	$(DBT) deps --project-dir $(DBT_PROJECT_DIR) --profiles-dir $(DBT_PROJECT_DIR)
