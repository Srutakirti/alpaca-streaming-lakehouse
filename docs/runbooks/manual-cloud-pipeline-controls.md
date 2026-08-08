# Manual Cloud Pipeline Controls

Use this when you want to manually start or stop the cloud pipeline infrastructure, or manually trigger the extractor, without waiting for the weekday schedules.

## Schedule

Configured Cloud Scheduler jobs:

| Job | Purpose | Schedule |
| --- | --- | --- |
| `alpaca-infra-start` | Starts Tansu VM, starts Cloud SQL, warms `alpaca-loader` | `09:05 America/New_York`, weekdays |
| `alpaca-extractor-start` | Runs the extractor Cloud Run job | `09:30 America/New_York`, weekdays |
| `alpaca-infra-stop` | Scales down loader, stops Cloud SQL, stops Tansu VM after the extractor idle-shutdown and loader drain window | `17:20 America/New_York`, weekdays |

## Manual Trigger Through Cloud Scheduler

Use this when you want to test the Scheduler job wiring itself.

Start infra:

```bash
gcloud scheduler jobs run alpaca-infra-start \
  --project=project-66783f65-9c3e-4880-9a3 \
  --location=us-east1
```

Run extractor:

```bash
gcloud scheduler jobs run alpaca-extractor-start \
  --project=project-66783f65-9c3e-4880-9a3 \
  --location=us-east1
```

Stop infra:

```bash
gcloud scheduler jobs run alpaca-infra-stop \
  --project=project-66783f65-9c3e-4880-9a3 \
  --location=us-east1
```

Cloud Scheduler returns quickly. Use the verification commands below to monitor the actual effect.

## Manual Trigger Directly

Use this when you want to run the underlying Cloud Run job and wait for completion from the terminal.

Start infra:

```bash
gcloud run jobs execute alpaca-infra-start \
  --project=project-66783f65-9c3e-4880-9a3 \
  --region=us-east1 \
  --wait
```

Run extractor:

```bash
gcloud run jobs execute alpaca-extractor \
  --project=project-66783f65-9c3e-4880-9a3 \
  --region=us-east1 \
  --wait
```

Stop infra:

```bash
gcloud run jobs execute alpaca-infra-stop \
  --project=project-66783f65-9c3e-4880-9a3 \
  --region=us-east1 \
  --wait
```

Recommended order for a manual full run:

```text
1. alpaca-infra-start
2. alpaca-extractor
3. alpaca-infra-stop
```

## Verify State

Check schedules:

```bash
gcloud scheduler jobs list \
  --project=project-66783f65-9c3e-4880-9a3 \
  --location=us-east1 \
  --format='table(name,state,schedule,timeZone)'
```

Check Tansu VM and static external IP:

```bash
gcloud compute instances list \
  --project=project-66783f65-9c3e-4880-9a3 \
  --format='table(name,zone,status,networkInterfaces[0].accessConfigs[0].natIP)'
```

Check reserved static IP:

```bash
gcloud compute addresses list \
  --project=project-66783f65-9c3e-4880-9a3 \
  --regions=us-east1 \
  --format='table(name,address,status,users)'
```

Check Cloud SQL:

```bash
gcloud sql instances list \
  --project=project-66783f65-9c3e-4880-9a3 \
  --format='table(name,state,settings.activationPolicy)'
```

Check recent Cloud Run job executions:

```bash
gcloud run jobs executions list \
  --project=project-66783f65-9c3e-4880-9a3 \
  --region=us-east1 \
  --limit=10 \
  --format='table(metadata.name,metadata.labels.run.googleapis.com/execution-generation,status.conditions[0].type,status.conditions[0].status,status.completionTime)'
```

Check loader service URL and health:

```bash
gcloud run services describe alpaca-loader \
  --project=project-66783f65-9c3e-4880-9a3 \
  --region=us-east1 \
  --format='value(status.url)'
```

Then call the returned service URL with `/health` if needed.

## Expected Idle State

After `alpaca-infra-stop`:

```text
tansu-broker               TERMINATED
alpaca-iceberg-catalog     STOPPED / NEVER
alpaca-loader minScale     0
```

## Expected Running State

After `alpaca-infra-start`:

```text
tansu-broker               RUNNING
tansu-broker external IP   34.138.155.73
alpaca-iceberg-catalog     RUNNABLE / ALWAYS
alpaca-loader minScale     1
```
