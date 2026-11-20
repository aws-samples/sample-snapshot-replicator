# Sample: Cross-Partition EBS Snapshot Replication

Sample code showing how to replicate Amazon EBS snapshots **across AWS partitions** (for example Commercial → GovCloud, Commercial → China, or Commercial → European Sovereign Cloud) using the [EBS Direct APIs](https://docs.aws.amazon.com/ebs/latest/userguide/ebs-accessing-snapshot.html).

> This is sample code, for non-production usage. You should work with your security and legal teams to meet your organizational security, regulatory and compliance requirements before deployment.

## What this sample demonstrates

Amazon EC2 already provides [`CopySnapshot`](https://docs.aws.amazon.com/AWSEC2/latest/APIReference/API_CopySnapshot.html), which copies a snapshot between regions **within the same partition** entirely server-side — no snapshot data flows through your machine. When `CopySnapshot` applies, prefer it: it is simpler, cheaper, and faster than this sample.

`CopySnapshot` cannot cross a partition boundary: credentials and the server-side copy path are scoped to a single partition. This sample shows one way to work around that gap by:

- Reading blocks from the source snapshot with `GetSnapshotBlock` using credentials in the source partition.
- Writing those blocks into a destination snapshot with `PutSnapshotBlock` using credentials in the target partition.
- Using the `StartSnapshot` client token for idempotent re-runs, plus a local state file for resume after an interruption.
- Preserving provenance via a `SourceSnapshotArn` tag applied at `StartSnapshot` time.

The sample is organized as a small Rust CLI, but the interesting pieces are the API usage patterns, the IAM/KMS model, and the resume strategy — not the binary itself.

## Supported AWS Partitions

- **Commercial (`aws`)**: `us-east-1`, `eu-west-1`, `ap-southeast-2`, etc.
- **GovCloud (`aws-us-gov`)**: `us-gov-west-1`, `us-gov-east-1`
- **China (`aws-cn`)**: `cn-north-1`, `cn-northwest-1`
- **European Sovereign Cloud (`aws-eusc`)**: `eusc-de-east-1`

## Deployment and Networking

All block data flows **through the machine running this sample**: it reads each
block from the source region with `GetSnapshotBlock` and writes it to the target
region with `PutSnapshotBlock`. That machine must be able to reach the EBS
Direct API endpoints of **both** partitions — either the public endpoints or
`com.amazonaws.<region>.ebs` interface VPC endpoints in each partition.

For cross-partition or cross-region copies, run the sample on an instance with
good network bandwidth to both endpoints (for example an EC2 instance in the
source region), since throughput is bounded by the round trip to each endpoint
and by the host's bandwidth. Peak in-flight block *data* is roughly
`concurrency × 512 KiB` (so the default of 100 is about 50 MiB). Separately, the
tool holds the full block list and per-block checksums in memory for the
duration of a run, so total memory also grows with the number of data blocks in
the snapshot (on the order of ~100 bytes per block for the block token and
checksum).

## Cost

The EBS Direct APIs are billed per request (`ListSnapshotBlocks`,
`ListChangedBlocks`, `GetSnapshotBlock`, `PutSnapshotBlock`), and
cross-region/cross-partition transfers incur data transfer charges. See the
[Amazon EBS pricing page](https://aws.amazon.com/ebs/pricing/) for current rates.

## Prerequisites

- Rust 1.94.1 or later
- AWS credentials configured for both source and target partitions
- IAM / KMS permissions (see [Permissions](#permissions))

## Building

```bash
git clone https://github.com/aws-samples/sample-snapshot-replicator.git
cd sample-snapshot-replicator
cargo build --release
```

The binary is at `target/release/snapshot-replicator`.

## Usage

### Same-partition cross-region replication

```bash
./target/release/snapshot-replicator \
  --snapshot-id snap-0123456789abcdef0 \
  --source-region us-east-1 \
  --target-region us-west-2 \
  --target-profile default
```

### Cross-partition replication

```bash
# Commercial to GovCloud
./target/release/snapshot-replicator \
  --snapshot-id snap-0123456789abcdef0 \
  --source-region us-east-1 \
  --source-profile commercial \
  --target-region us-gov-west-1 \
  --target-profile govcloud

# Commercial to European Sovereign Cloud
./target/release/snapshot-replicator \
  --snapshot-id snap-0123456789abcdef0 \
  --source-region eu-west-1 \
  --source-profile commercial \
  --target-region eusc-de-east-1 \
  --target-profile eusc
```

Both profiles must be configured in your AWS credentials file.

### With performance tuning

```bash
./target/release/snapshot-replicator \
  --snapshot-id snap-0123456789abcdef0 \
  --source-region us-east-1 \
  --target-region us-west-2 \
  --target-profile default \
  --concurrency 200 \
  --max-attempts 8
```

### Incremental replication

If you have already replicated an earlier snapshot of the same volume, you can copy only the blocks that changed since then. This transfers far less data for repeated backups of the same volume.

You supply two parent IDs:

- `--parent-snapshot-id` — the earlier **source** snapshot (the one you previously replicated)
- `--parent-dest-snapshot-id` — the **destination** snapshot that parent was replicated to

The sample calls `ListChangedBlocks(parent, source)` to find the changed blocks, copies only those, and bases the new destination snapshot on the parent via `StartSnapshot`'s `ParentSnapshotId`. Both flags must be provided together.

```bash
./target/release/snapshot-replicator \
  --snapshot-id snap-0new0new0new0new0 \
  --source-region us-east-1 \
  --target-region us-west-2 \
  --target-profile default \
  --parent-snapshot-id snap-0old0old0old0old0 \
  --parent-dest-snapshot-id snap-0destdestdestdes0
```

> **Note**: This is a stateless, manual incremental copy — you provide the lineage. The sample does not track parent/child relationships across runs. You are responsible for ensuring the parent source and parent destination snapshots correspond to the same volume lineage and that the parent destination snapshot still exists and is in the `completed` state.

## Command-Line Options

| Option | Description | Required | Default |
|--------|-------------|----------|---------|
| `--snapshot-id`, `-s` | Source EBS snapshot ID (e.g., snap-0123456789abcdef0) | Yes | - |
| `--source-region` | AWS region where the source snapshot lives | Yes | - |
| `--target-region`, `-t` | Target AWS region for the replicated snapshot | Yes | - |
| `--target-profile`, `-p` | AWS profile name for the target partition | Yes | - |
| `--source-profile` | AWS profile name for the source partition | No | default |
| `--concurrency`, `-c` | Number of concurrent block copy operations | No | 100 |
| `--max-attempts` | Max attempts per block API call for the SDK's adaptive retryer (first try + retries) | No | 5 |
| `--encrypt` | Encrypt the destination snapshot (uses the target account's default EBS key unless `--kms-key-id` is given) | No | target account default |
| `--kms-key-id` | Target KMS key ARN for destination encryption (implies `--encrypt`) | No | - |
| `--parent-snapshot-id` | Parent source snapshot for incremental copy (requires `--parent-dest-snapshot-id`) | No | - |
| `--parent-dest-snapshot-id` | Destination snapshot the parent was replicated to (requires `--parent-snapshot-id`) | No | - |

## Performance Tuning

Two optional flags control throughput and resilience:

- **`--concurrency <N>`** — number of blocks copied in parallel (default `100`, max `10000`). Higher values increase throughput but use more memory and network bandwidth, and are more likely to hit EBS API throttling.
- **`--max-attempts <N>`** — maximum attempts per block API call for the AWS SDK's adaptive retryer (first try + retries; default `5`, max `20`). Transient failures (throttling, timeouts) are retried inside each `GetSnapshotBlock` / `PutSnapshotBlock` call by the SDK. The copy itself makes a **single concurrent pass**: a block error that survives the SDK's retries is treated as systemic (for example an IAM or KMS denial, or a destination that is already completed) and stops the run — new work stops immediately, in-flight requests settle, and the first error is returned. Everything copied so far is in the state file, so re-running the same command resumes the remainder.

## Permissions

The policies below are starting points. Scope them further for your environment: in particular, replace the placeholder ARNs with the specific snapshot ARNs you intend to use.

### Source Partition IAM Permissions

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Sid": "ReadSourceSnapshot",
      "Effect": "Allow",
      "Action": [
        "ebs:ListSnapshotBlocks",
        "ebs:ListChangedBlocks",
        "ebs:GetSnapshotBlock"
      ],
      "Resource": "arn:<source-partition>:ec2:<source-region>::snapshot/<source-snapshot-id>"
    },
    {
      "Sid": "DescribeSourceSnapshot",
      "Effect": "Allow",
      "Action": "ec2:DescribeSnapshots",
      "Resource": "*"
    }
  ]
}
```

`ec2:DescribeSnapshots` does not support resource-level permissions and must be granted on `"*"`.

### Target Partition IAM Permissions

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Sid": "CreateDestinationSnapshot",
      "Effect": "Allow",
      "Action": [
        "ebs:StartSnapshot",
        "ebs:PutSnapshotBlock",
        "ebs:CompleteSnapshot"
      ],
      "Resource": "arn:<target-partition>:ec2:<target-region>::snapshot/*"
    },
    {
      "Sid": "TagOnCreate",
      "Effect": "Allow",
      "Action": "ec2:CreateTags",
      "Resource": "arn:<target-partition>:ec2:<target-region>:*:snapshot/*",
      "Condition": {
        "StringEquals": {
          "ec2:CreateAction": "StartSnapshot"
        }
      }
    },
    {
      "Sid": "DescribeDestinationSnapshot",
      "Effect": "Allow",
      "Action": "ec2:DescribeSnapshots",
      "Resource": "*"
    }
  ]
}
```

`ec2:CreateTags` is required because the sample applies tags (including the
`SourceSnapshotArn` provenance tag) directly on the `StartSnapshot` request, and
`StartSnapshot` is the only EBS direct API action that can specify tags. The
condition restricts tagging to the create action so the role cannot tag
arbitrary existing snapshots.

**Note**: For encrypted snapshots, additional KMS permissions are required (see below).

## Encryption

### How encryption is handled

The sample never performs client-side encryption or decryption. All cryptographic operations happen server-side within EBS:

1. **Source (read)**: The EBS Direct API (`GetSnapshotBlock`) transparently decrypts block data server-side using the source snapshot's KMS key (if it has one). The block data is returned over HTTPS.
2. **In transit**: All API calls use HTTPS (TLS), so block data is encrypted in transit between the source region, the machine running the sample, and the destination region.
3. **Destination (write)**: EBS encrypts each block server-side as it is written via `PutSnapshotBlock`, if the destination snapshot is encrypted (see below).

### Destination encryption and KMS key behavior

The source snapshot's encryption state and KMS key are **irrelevant** to the destination. By default (no `--encrypt` / `--kms-key-id` flags), the sample omits both `Encrypted` and `KmsKeyArn` from `StartSnapshot`, so the **target account's settings in the target region** decide the outcome:

| Target account's [EBS encryption by default](https://docs.aws.amazon.com/ebs/latest/userguide/encryption-by-default.html) (in the target region) | Destination snapshot |
|---|---|
| Enabled | Encrypted with the account's default EBS KMS key (the AWS managed `aws/ebs` key or the customer managed key configured as the default) |
| Disabled | Unencrypted |

This applies regardless of whether the source is encrypted, and in both same-region and cross-region copies. In particular, an **encrypted source can produce an unencrypted replica** if the target account does not have encryption by default enabled. Use `--encrypt` or `--kms-key-id` if the replica must always be encrypted.

You can override this behavior from the CLI:

- **`--encrypt`**: Requests an encrypted destination snapshot using the target account's **default EBS key**, whatever the account's encryption-by-default setting.
- **`--kms-key-id <ARN>`**: Forces an encrypted destination snapshot using the **KMS key you specify** in the target region/partition. This implies `--encrypt`. The ARN must be valid in the target partition (e.g. `arn:aws:kms:...`, `arn:aws-us-gov:kms:...`, or `arn:aws-cn:kms:...`) and the target profile must have permission to use it.

If the target profile lacks permission to use the specified key, the sample fails fast with a `KmsAccessDenied` error naming the key and the required `kms:CreateGrant` / `kms:Decrypt` permissions.

### Encryption examples

```bash
# Encrypt with the target account's default EBS key
./target/release/snapshot-replicator \
  --snapshot-id snap-0123456789abcdef0 \
  --source-region us-east-1 \
  --target-region us-west-2 \
  --target-profile default \
  --encrypt

# Encrypt with a specific target KMS key (implies --encrypt)
./target/release/snapshot-replicator \
  --snapshot-id snap-0123456789abcdef0 \
  --source-region us-east-1 \
  --target-region us-west-2 \
  --target-profile default \
  --kms-key-id arn:aws:kms:us-west-2:123456789012:key/12345678-1234-1234-1234-123456789012
```

### Required KMS permissions

Validate the exact action set against the key policy of the specific KMS key you
intend to use. These are starting points.

**Source partition** (in addition to EBS permissions, only if the source snapshot is encrypted):

```json
{
  "Sid": "DecryptSourceSnapshot",
  "Effect": "Allow",
  "Action": [
    "kms:Decrypt",
    "kms:DescribeKey"
  ],
  "Resource": "<source-kms-key-arn>",
  "Condition": {
    "StringEquals": {
      "kms:ViaService": "ec2.<source-region>.amazonaws.com"
    }
  }
}
```

**Target partition** (in addition to EBS permissions, only if the destination will be encrypted):

```json
{
  "Sid": "EncryptDestinationSnapshot",
  "Effect": "Allow",
  "Action": [
    "kms:CreateGrant",
    "kms:Decrypt",
    "kms:DescribeKey",
    "kms:GenerateDataKeyWithoutPlaintext"
  ],
  "Resource": "<target-kms-key-arn-or-default-ebs-key>",
  "Condition": {
    "StringEquals": {
      "kms:ViaService": "ec2.<target-region>.amazonaws.com"
    }
  }
}
```

### Unencrypted snapshots

With no encryption flags, the destination is unencrypted only if the target account has [EBS encryption by default](https://docs.aws.amazon.com/ebs/latest/userguide/encryption-by-default.html) disabled in the target region. The source snapshot's encryption state makes no difference. See [Destination encryption and KMS key behavior](#destination-encryption-and-kms-key-behavior).

The sample never sends `Encrypted=false`: EBS rejects that request when encryption by default is enabled on the account.

## How It Works

1. **Validation**: Validates source and target region formats.
2. **Authentication**: Creates AWS clients for both source and target partitions.
3. **Metadata retrieval**: Fetches source snapshot metadata (size, description, tags).
4. **Block discovery**: Lists all data blocks in the source snapshot (`ListSnapshotBlocks`) or only the changed blocks for an incremental copy (`ListChangedBlocks`).
5. **Destination creation**: Creates a new snapshot in the target region with the source's description, its user tags, and a `SourceSnapshotArn` provenance tag applied at creation time via `StartSnapshot`.
6. **Concurrent copying**: Copies blocks in parallel with configurable concurrency.
7. **Completion**: Finalizes the destination snapshot with an aggregate checksum via `CompleteSnapshot`.

> Source tags are copied across the partition boundary. Review your source snapshot's tags before replicating in case they hold values you do not want to carry across accounts or partitions.

### Automatic resume

If a replication is interrupted (network failure, crash, Ctrl+C), re-running the **exact same command** automatically resumes from where it left off — no extra flags needed.

How it works:

1. **Snapshot rediscovery**: `StartSnapshot` is called with a deterministic client token derived from the source snapshot ID and target region. If a pending destination snapshot already exists from a previous run, EBS returns it instead of creating a new one.
2. **Block tracking**: As each block is successfully copied, its index and checksum are appended to a local state file (`./snapshot-replicator-state-<dest-snapshot-id>.csv`). This file survives crashes because it is append-only.
3. **On resume**: The state file is loaded, already-copied blocks are skipped, and only the remaining blocks are transferred. The progress bar starts at the resumed position immediately.
4. **Completion**: Once all blocks are copied, the state file is deleted automatically.

If the state file is lost or missing, the sample falls back to copying all blocks from scratch — this is safe because `PutSnapshotBlock` is idempotent.

Re-running the **exact same command after a successful replication** is also safe: the deterministic client token makes `StartSnapshot` return the already-completed destination snapshot, so the sample reports the same destination snapshot ID and does no further work — it does **not** create a duplicate. (The token is derived from the source snapshot ID, target region, KMS key, and parent snapshot, so changing any of those produces a new, independent copy.)

## Creating an AMI from a Replicated Snapshot

This sample replicates the **snapshot** only, not an AMI. If the source snapshot
was the root volume of an AMI and you want a bootable image in the target
region, register one from the replicated snapshot with `ec2:RegisterImage`:

```bash
aws ec2 register-image \
  --region <target-region> \
  --profile <target-profile> \
  --name "my-replicated-image" \
  --architecture x86_64 \
  --root-device-name /dev/xvda \
  --block-device-mappings '[{"DeviceName":"/dev/xvda","Ebs":{"SnapshotId":"<dest-snapshot-id>"}}]' \
  --ena-support \
  --boot-mode uefi
```

Match the architecture, root device name, boot mode (`uefi` / `legacy-bios` /
`uefi-preferred`), ENA/SR-IOV, and virtualization type to the original image, and
add mappings for any non-root volumes. These attributes are not carried by the
snapshot itself, so you must supply them from the source AMI's description.

## Limitations

- **Encryption**: By default, cross-region encrypted snapshots use the default EBS encryption key in the target region. Use `--kms-key-id` to specify a custom target key.
- **KMS Keys**: The *source* snapshot's KMS key cannot be reused across regions; the destination key must exist in the target region/partition.
- **Completion Verification**: After `CompleteSnapshot`, the sample polls `DescribeSnapshots` in the target region for up to 5 minutes (with exponential backoff from 2 s to a 15 s cap, so fast completions are detected in a couple of seconds) for the snapshot to reach `completed`. If the target profile lacks `ec2:DescribeSnapshots`, or the snapshot is still finalizing after 5 minutes, the sample exits non-zero even though the block copy itself succeeded. Verify the destination snapshot state manually in that case (`aws ec2 describe-snapshots --snapshot-ids <dest-id> --region <target-region> --profile <target-profile>`).
- **Block token lifetime**: Block tokens returned by `ListSnapshotBlocks` / `ListChangedBlocks` have a server-side expiry. For very long runs, late `GetSnapshotBlock` calls may fail on expired tokens; re-running the sample re-lists the blocks and resumes.
- **AWS Service Limits**: Subject to EBS Direct API rate limits and quotas.

## Troubleshooting

### "Invalid region format" Error

Ensure the region follows the correct format for your partition:
- Commercial: `us-east-1`, `eu-west-1`
- GovCloud: `us-gov-west-1`
- China: `cn-north-1`
- European Sovereign: `eusc-de-east-1`

### Authentication Errors

Verify your AWS credentials are properly configured:

```bash
# Check source credentials
aws sts get-caller-identity --profile <source-profile>

# Check target credentials
aws sts get-caller-identity --profile <target-profile>
```

## Development

### Building

```bash
cargo build --release
```

### Running tests

```bash
cargo test
```

The integration tests in `tests/integration_test.rs` hit real AWS and are
`#[ignore]`d by default. To run them, set the required environment variables and
pass `--ignored`:

```bash
TEST_SOURCE_SNAPSHOT_ID=snap-0123456789abcdef0 \
TEST_TARGET_PROFILE=prod \
TEST_SOURCE_REGION=us-east-1 \
TEST_TARGET_REGION=us-west-2 \
TEST_SOURCE_PROFILE=commercial \
cargo test -- --ignored
```

`TEST_SOURCE_SNAPSHOT_ID` and `TEST_TARGET_PROFILE` are required; the others fall
back to defaults (`us-east-1` / `us-west-2` / the default profile).

### Running with debug logging

```bash
RUST_LOG=debug ./target/release/snapshot-replicator \
  --snapshot-id snap-0123456789abcdef0 \
  --source-region us-east-1 \
  --target-region us-west-2 \
  --target-profile default
```

## Architecture

The sample is organized into small modules so each piece can be read on its own:

- **CLI**: Command-line argument parsing and validation
- **Auth**: AWS authentication and client creation
- **Metadata**: Snapshot metadata operations
- **Snapshot**: Snapshot creation and replication orchestration (with resume)
- **Block Copier**: Individual block copy operations
- **Checksum**: Aggregate checksum calculation
- **Progress**: Real-time progress reporting
- **Region**: Multi-partition region validation
- **State**: Resume state file persistence

Transient failures (throttling, timeouts) are handled by the AWS SDK's adaptive
retryer configured at client creation; a block error that survives those
retries stops the single-pass copy (fail fast).

## Security

See [SECURITY.md](SECURITY.md) and
[CONTRIBUTING](CONTRIBUTING.md#security-issue-notifications) for information on
reporting security issues.

This is sample code, for non-production usage. You should work with your
security and legal teams to meet your organizational security, regulatory and
compliance requirements before deployment.

## License

This project is licensed under the MIT-0 License. See the [LICENSE](LICENSE) file for details.
