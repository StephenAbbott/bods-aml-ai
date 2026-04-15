# bods-aml-ai

Transform [Beneficial Ownership Data Standard (BODS) v0.4](https://standard.openownership.org/en/0.4.0/) data into [Google Anti Money Laundering AI](https://docs.cloud.google.com/financial-services/anti-money-laundering/docs/reference/schemas/aml-input-data-model) input format.

## Why this exists

Google's Anti Money Laundering AI scores party-level risk based on transaction patterns, account holdings, and historical investigation outcomes. Beneficial ownership data — who ultimately owns and controls companies — is a critical input for AML risk assessment, but Google's AML AI data model has **no native concept of party-to-party ownership relationships**.

This tool bridges that gap by transforming BODS ownership data into the AML AI input tables, encoding beneficial ownership signals through the available extensibility mechanisms.

## The mapping challenge

The AML AI input data model is **account-centric and transaction-centric**. It defines six tables:

| Table | Purpose | Relevance to BODS |
|---|---|---|
| **Party** | Customer entities (COMPANY/CONSUMER) | Direct mapping from BODS entity/person statements |
| **AccountPartyLink** | Links parties to accounts | Only native relationship mechanism — used for synthetic ownership links |
| **PartySupplementaryData** | Custom numeric attributes per party | Used to encode ownership %, BO flags, interest types |
| Transaction | Financial transactions | Not populated from BODS data |
| RiskCaseEvent | AML investigation events | Not populated from BODS data |
| InteractionEvent | Non-financial interactions | Not populated from BODS data |

The core structural mismatch: **AML AI has no Party-to-Party relationship table**. BODS ownership-or-control statements — the heart of beneficial ownership data — must be decomposed into party-level attributes.

## How the mapping works

### BODS Person Statement → Party (type=CONSUMER)

| BODS field | AML AI field |
|---|---|
| `recordId` / `statementId` | `party_id` |
| `statementDate` | `validity_start_time` |
| `recordDetails.names[].fullName` | `name` |
| `recordDetails.birthDate` | `birth_date` |
| `recordDetails.nationalities[].code` | `nationalities[].region_code` |
| `recordDetails.addresses[]` | `addresses[]` |

### BODS Entity Statement → Party (type=COMPANY)

| BODS field | AML AI field |
|---|---|
| `recordId` / `statementId` | `party_id` |
| `statementDate` | `validity_start_time` |
| `recordDetails.name` | `name` |
| `recordDetails.foundingDate` | `establishment_date` |
| `recordDetails.jurisdiction.code` | `residencies[].region_code` |
| `recordDetails.addresses[]` | `addresses[]` |

### BODS Ownership-or-Control Statement → PartySupplementaryData

Since AML AI only supports `float64` values in supplementary data, ownership relationships are decomposed into numeric signals:

**Per interested party (the owner/controller):**

| Supplementary data ID | Value | Description |
|---|---|---|
| `bo_ownership_pct_{subject_id}` | Ownership percentage (e.g. 60.0) | How much of the subject this party owns |
| `bo_is_beneficial_owner` | 1.0 or absent | Whether this party is flagged as a beneficial owner |
| `bo_is_direct` | 1.0 (direct) / 0.0 (indirect) | Whether the ownership is direct or indirect |
| `bo_interest_type_{type}` | 1.0 | Flag for each BODS interest type (shareholding, votingRights, etc.) |

**Per subject entity (the company being owned):**

| Supplementary data ID | Value | Description |
|---|---|---|
| `bo_num_owners` | Count (e.g. 2.0) | Number of identified interested parties |
| `bo_total_identified_ownership_pct` | Sum (e.g. 100.0) | Total identified ownership percentage |
| `bo_has_beneficial_owner` | 1.0 or absent | Whether any beneficial owner has been identified |

### BODS Ownership-or-Control Statement → AccountPartyLink (optional)

As an additional mechanism, synthetic "ownership accounts" can be created to represent ownership relationships through AML AI's only native relationship table:

- Each owned entity (subject) gets a synthetic account (`bods-ownership-{subject_id}`)
- The subject entity is linked as `PRIMARY_HOLDER`
- Each interested party is linked as `SUPPLEMENTARY_HOLDER`

This allows AML AI's graph-based risk scoring to detect connections between parties sharing ownership relationships, even though AccountPartyLink was designed for bank accounts.

Use `--no-account-links` to skip this if you only want Party + PartySupplementaryData.

## Installation

```bash
pip install -e .
```

For development (includes pytest and lib-cove-bods):

```bash
pip install -e ".[dev]"
```

## Usage

### Transform a single file

```bash
bods-aml-ai transform sample_input.json -o output/
```

### Transform a directory of files

```bash
bods-aml-ai batch data/ -o output/
```

### Skip synthetic account links

```bash
bods-aml-ai transform sample_input.json -o output/ --no-account-links
```

### Verbose logging

```bash
bods-aml-ai -v transform sample_input.json -o output/
```

### Output

The tool produces NDJSON (newline-delimited JSON) files in the output directory, one per AML AI table:

```
output/
  party.ndjson
  party_supplementary_data.ndjson
  account_party_link.ndjson
```

These files can be loaded directly into BigQuery using `bq load --source_format=NEWLINE_DELIMITED_JSON`.

## Example

Given the `sample_input.json` containing:
- **Acme Holdings Ltd** (UK company, GB-COH-12345678)
- **Caribbean Investments BVI Ltd** (BVI company, VG-FSC-98765)
- **Jane Smith** (UK national, born 1980) — owns 60% of Acme directly, is a beneficial owner
- **Hans Mueller** (German national, born 1975) — owns 100% of BVI company, is a beneficial owner
- BVI company owns 40% of Acme (corporate shareholding, not a beneficial owner)

The transformation produces:

**party.ndjson** — 4 rows (2 COMPANY, 2 CONSUMER)

```json
{"party_id": "gb-coh-12345678", "type": "COMPANY", "name": "Acme Holdings Ltd", "establishment_date": "2015-03-20", ...}
{"party_id": "vg-fsc-98765", "type": "COMPANY", "name": "Caribbean Investments BVI Ltd", ...}
{"party_id": "person-jane-smith-1980", "type": "CONSUMER", "name": "Jane Smith", "birth_date": "1980-05-14", ...}
{"party_id": "person-hans-mueller-1975", "type": "CONSUMER", "name": "Hans Mueller", ...}
```

**party_supplementary_data.ndjson** — ownership signals as numeric attributes

```json
{"party_supplementary_data_id": "bo_ownership_pct_gb-coh-12345678", "party_id": "person-jane-smith-1980", "supplementary_data_payload": {"value": 60.0}, ...}
{"party_supplementary_data_id": "bo_is_beneficial_owner", "party_id": "person-jane-smith-1980", "supplementary_data_payload": {"value": 1.0}, ...}
{"party_supplementary_data_id": "bo_num_owners", "party_id": "gb-coh-12345678", "supplementary_data_payload": {"value": 2.0}, ...}
```

**account_party_link.ndjson** — synthetic ownership accounts

```json
{"account_id": "bods-ownership-gb-coh-12345678", "party_id": "gb-coh-12345678", "role": "PRIMARY_HOLDER", ...}
{"account_id": "bods-ownership-gb-coh-12345678", "party_id": "person-jane-smith-1980", "role": "SUPPLEMENTARY_HOLDER", ...}
{"account_id": "bods-ownership-gb-coh-12345678", "party_id": "vg-fsc-98765", "role": "SUPPLEMENTARY_HOLDER", ...}
```

## Loading into BigQuery

```bash
# Create dataset
bq mk --dataset my_project:aml_input

# Load each table
bq load --source_format=NEWLINE_DELIMITED_JSON \
  my_project:aml_input.party \
  output/party.ndjson

bq load --source_format=NEWLINE_DELIMITED_JSON \
  my_project:aml_input.party_supplementary_data \
  output/party_supplementary_data.ndjson

bq load --source_format=NEWLINE_DELIMITED_JSON \
  my_project:aml_input.account_party_link \
  output/account_party_link.ndjson
```

## Limitations and design decisions

1. **No native ownership relationships**: AML AI cannot natively represent "Party A owns X% of Party B". Ownership data is decomposed into per-party numeric attributes (PartySupplementaryData) and optional synthetic account links (AccountPartyLink).

2. **Ownership chains are flattened**: BODS supports indirect ownership chains via `isComponent` and `componentStatementIDs`. Component statements (intermediate chain pieces) are skipped — only top-level relationships are transformed. The chain structure is lost in translation.

3. **PartySupplementaryData constraints**: Values must be `float64`. Maximum 100 supplementary data IDs per party. Parties with complex ownership structures (many owners across many entities) may approach this limit.

4. **No transaction or investigation data**: BODS is a structural data standard — it describes who owns what. AML AI's Transaction, RiskCaseEvent, and InteractionEvent tables must be populated from other sources (banking systems, investigation workflows).

5. **Temporal model**: BODS uses `replacesStatements` for versioning; AML AI uses `validity_start_time`. This tool uses `statementDate` as `validity_start_time` but does not resolve `replacesStatements` chains — all statements are treated as current.

6. **Synthetic accounts are semantically imperfect**: AccountPartyLink was designed for bank accounts with holder roles. Using it for ownership relationships (subject = PRIMARY_HOLDER, owner = SUPPLEMENTARY_HOLDER) is a workaround, not a semantic match.

## Testing

```bash
pytest tests/ -v
```

## Project structure

```
src/bods_aml_ai/
  cli.py                          # Click CLI (transform / batch commands)
  config.py                       # TransformConfig dataclass
  pipeline.py                     # AMLPipeline orchestrator
  ingestion/
    bods_reader.py                # Read BODS JSON/JSONL files
    models.py                     # Lightweight BODS statement models
  transform/
    parties.py                    # Person/Entity -> Party rows
    supplementary.py              # Relationships -> PartySupplementaryData rows
    account_party_links.py        # Relationships -> AccountPartyLink rows (optional)
  output/
    writer.py                     # NDJSON file writer (one file per table)
  utils/
    countries.py                  # Country code resolution (pycountry)
    dates.py                      # Date/timestamp conversion
```

## References

- [BODS v0.4 documentation](https://standard.openownership.org/en/0.4.0/)
- [BODS schema reference](https://standard.openownership.org/en/0.4.0/standard/reference.html)
- [Google AML AI input data model](https://docs.cloud.google.com/financial-services/anti-money-laundering/docs/reference/schemas/aml-input-data-model)
- [Google AML AI documentation](https://cloud.google.com/financial-services/anti-money-laundering/docs)

## License

MIT
