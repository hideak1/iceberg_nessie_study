.PHONY: help install vendor serve build check counts diagrams nav clean deploy

help: ## Show available targets
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) \
		| awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-10s\033[0m %s\n", $$1, $$2}'

install: ## Install python deps with uv
	uv sync

vendor: ## Clone the pinned upstream sources into vendor/
	./scripts/vendor.sh

serve: ## Serve the book at http://localhost:8000
	uv run mkdocs serve

build: ## Build the static site into site/
	uv run mkdocs build --strict

counts: ## List counting claims the snippet on the same page contradicts
	uv run python scripts/check_counts.py

diagrams: ## Validate every mermaid diagram with mermaid itself
	@node scripts/check_mermaid.mjs

nav: ## Regenerate the nav and part contents tables from the chapters on disk
	uv run python scripts/gen_nav.py
	uv run python scripts/gen_indexes.py

check: ## Verify locators, hand-typed source, cross-references, nav and contents
	uv run python hooks/snippets.py
	uv run python scripts/check_iron_rule.py
	uv run python scripts/check_refs.py --strict
	uv run python scripts/check_docs_tree.py --strict
	uv run python scripts/gen_nav.py --check
	uv run python scripts/gen_indexes.py --check

deploy: ## Publish to GitHub Pages
	uv run mkdocs gh-deploy --force

clean: ## Remove build output
	rm -rf site/ __pycache__/ hooks/__pycache__/
