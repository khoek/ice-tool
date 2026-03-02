# ice

CLI to manage cloud VMs, including marketplace offer selection and one-shot setup.

## Install

```bash
cargo install --path .
```

## Clouds

Supported cloud identifiers:

- `vast.ai`
- `gcp`
- `aws`

## Config File

`ice` stores config in `~/.ice/config.toml`.

Key groups:

- `default.*`: defaults used by commands when flags are omitted
- `auth.*`: per-cloud credentials

### Config Keys

Current `ice config set KEY=VALUE` keys:

- `default.cloud`: `vast.ai|gcp|aws`
- `default.min_cpus`: integer
- `default.min_ram_gb`: integer
- `default.allowed_gpus`: comma-separated GPU list
- `default.max_price_per_hr`: float USD/hour (effective cap over requested runtime)
- `default.setup.action`: `none|repo` (empty value unsets)
- `default.setup.repo_url`: git clone URL (empty value unsets)
- `auth.vast_ai.api_key`: Vast API key
- `auth.gcp.project|service_account_json`
- `auth.aws.access_key_id|secret_access_key`
- `default.gcp.region|zone|image_family|image_project|boot_disk_gb`
- `default.aws.region|ami|key_name|ssh_key_path|ssh_user|security_group_id|subnet_id|root_disk_gb`

## Commands

### `ice login --cloud CLOUD`

Ensures credentials are saved in `~/.ice/config.toml` for `CLOUD`.

- If missing, opens browser to cloud auth/API key page and prompts for credential input.
- For `vast.ai`, opens `https://cloud.vast.ai/manage-keys/` so API key creation/copy is direct.
- For `vast.ai`, validates the API key against the Vast user endpoint before saving.
- For `gcp`, prompts only for project ID and service-account JSON path (no required zone pin).
- For `aws`, prompts only for access key ID and secret access key (no required region pin).
- If `default.cloud` is set, `--cloud` can be omitted.

### `ice config set KEY=VALUE`

Sets supported keys listed above.

### `ice config get KEY`

Reads a single config key.

- prints `<unset>` when missing
- secret auth values are redacted

### `ice config list`

Lists all supported config keys and their current values.

### `ice list --cloud CLOUD`

Lists current instances created by `ice` on `CLOUD`.

- Instances are identified via an unambiguous naming prefix (`ice-...`) in labels.
- Output includes instance id/label/status, running hours (2dp), health hint, and hourly price when available.

### `ice shell --cloud CLOUD INSTANCE` / `ice sh --cloud CLOUD INSTANCE`

Opens SSH shell into `INSTANCE`.

- If target is stopped, prompts to start first or abort.

### `ice dl --cloud CLOUD INSTANCE REMOTE_PATH [LOCAL_PATH]`

Downloads file/dir from instance.

- `vast.ai`/`aws`: uses `rsync` over SSH
- `gcp`: uses `gcloud compute scp`
- destination defaults to current local directory when `LOCAL_PATH` omitted

### `ice stop --cloud CLOUD INSTANCE`

Stops instance if supported.

- Emits a prominent red error if provider/instance cannot be stopped.

### `ice start --cloud CLOUD INSTANCE`

Starts instance if supported.

- Reverse of `stop`.

### `ice delete --cloud CLOUD INSTANCE`

Stops first, then deletes instance.

- Emits prominent red error if safe stop/delete path is unsupported.

### `ice create --cloud CLOUD HOURS [--machine MACHINE] [--custom] [--dry-run]`

Most advanced command: finds and offers to create the cheapest matching instance for at least `HOURS`.

- `HOURS` accepts floats (for example `0.1`).

First-use (or missing defaults) interactive setup for:

- `default.min_cpus` (validated integer)
- `default.min_ram_gb` (validated integer)
- `default.allowed_gpus` (interactive checklist from cloud machine catalog / Vast GPU model list)
- `default.max_price_per_hr` (validated float)

Search behavior:

- Uses one simple internal machine model across all clouds: machine type, vCPU, RAM, GPU family, region, estimated hourly cost.
- `vast.ai`: queries marketplace offers meeting min CPU/RAM/GPU constraints and duration `>= HOURS`, then chooses cheapest.
- For `vast.ai`, after create `ice` schedules auto-stop using Vast scheduled jobs at the earliest hourly tick that is still `>= HOURS` from creation time.
- `gcp`/`aws`: searches built-in cloud machine catalogs across regions/configs and chooses cheapest match.
- Region/zone are only pinned when explicitly set via `default.gcp.region` / `default.gcp.zone` / `default.aws.region`.
- `--machine MACHINE`: optional cloud-specific machine type override (for example `g2-standard-4`, `g5.xlarge`).
- `--custom`: always prompt interactively for search filters (min CPU/RAM, allowed GPUs, max price), seeded from current defaults.
- If cheapest exceeds `default.max_price_per_hr`, fails and reports cheapest found price and estimated total.

Decision prompt options:

- `yes (default setup)`
- `yes (custom setup)`
- `no`
- `change filter` (interactively edit min/max filters and re-search)
- Prompt includes requested hours, billable hours (after runtime rounding), and estimated total cost.

`--dry-run`:

- Performs full search and reporting flow
- Stops before any accept/pay/create prompt

Post-create setup:

- If default setup chosen:
  - `default.setup.action=none`: do nothing
  - `default.setup.action=repo` (or unset): clone `default.setup.repo_url` into home directory
  - default setup is unavailable unless required default setup keys are present
- If custom setup chosen:
  - interactive prompts for action/repo URL
  - when setting values whose defaults are currently unset, prompts whether to persist as defaults

After setup completes, prompts whether to open an interactive shell in the new instance.

## Notes

- `--cloud` is optional when `default.cloud` is set.
- `ice` uses progress spinners (`indicatif`) for long-running operations.
- External tools used by some commands: `ssh`, `rsync`, `git`, `gcloud`, `aws`.
