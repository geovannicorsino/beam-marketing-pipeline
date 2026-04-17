# CLAUDE.md

For full project context see @CONTEXT.md.
For architecture and pipeline flow see @ARCHITECTURE.md.

## Code style

- Follow PEP 8 and Python 3.13 conventions
- Linting via Ruff — run `ruff check .` before suggesting changes
- Type hints on all functions and class methods
- `@dataclass` for data models — no Pydantic unless explicitly added

## Project structure

```
pipeline/
├── main.py              # pipeline entry point
├── options.py           # MarketingPipelineOptions
├── schemas/             # TableRecord (single data model)
├── sources/             # read_ga4, read_crm
├── normalize/           # NormalizeGA4Fn, NormalizeCRMFn
└── transforms/          # JoinAnalyticsCRMFn, ClassifyLeadFn
config/
└── lead_classification_rules.json
data/fixtures/           # local test data (GA4 JSON, CRM CSV)
```

## Running locally

Uses VS Code launch.json with DirectRunner. Parameters passed via `--args` in `.vscode/launch.json`. No GCS needed for local runs — sources point to `data/fixtures/`.

## Key conventions

- Sources return `PCollection[dict]` — normalization converts to `TableRecord`
- CRM is the primary entity in the join — GA4 enriches with `campaign_name`
- `TableRecord` fields not populated by a source default to `""`, `0`, or `None`
- Classification only applies to records where `source_system == "crm"`
- `setup()` is the correct place to load files in DoFns — not `__init__`
