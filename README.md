# Multi-Cloud FinOps ETL Pipeline

**Stack:** Python, pandas, boto3, google-cloud-bigquery, azure-storage-blob, oci

---

## What's Here

A multi-cloud cost data ETL pipeline that normalizes billing data from AWS, GCP, Azure, and Oracle Cloud into the FOCUS (FinOps Open Cost and Usage Specification) format, then loads it into your data warehouse of choice.

One file, 357 lines. Four cloud providers in, one schema out.

## Architecture

```
AWS CUR ──────────┐
GCP BigQuery ─────┤
Azure Blob ───────┤──→ FOCUS Normalization ──→ Service Categorization ──→ Warehouse
OCI Object Store ─┘                                                        ├── BigQuery
                                                                           ├── Redshift
                                                                           └── Snowflake
```

The pipeline is a single `MultiCloudFinOpsETL` class with three stages:

**Extract.** Each cloud provider has its own extraction method. AWS reads Cost and Usage Reports from S3 via boto3. GCP queries the billing export table in BigQuery. Azure pulls from Blob Storage. OCI reads from Object Storage.

**Transform.** Raw billing data gets normalized into FOCUS columns: `BillingPeriodStart`, `ServiceName`, `ServiceCategory`, `ResourceId`, `UsageAmount`, `BilledCost`, `EffectiveCost`, `Provider`. The service categorization engine maps provider-specific service names (like `AmazonEC2`, `Compute Engine`, `Virtual Machines`, `Compute`) into unified categories: Compute, Storage, Networking, Database, Analytics, Security, Management, Application.

**Load.** Normalized data goes into BigQuery, Redshift, or Snowflake. Each warehouse has its own loading method with bulk insert handling tuned to the target.

## Why FOCUS

The FinOps Foundation published the FOCUS spec to solve this exact problem: every cloud provider reports costs differently. AWS has line item types and blended rates. GCP has SKU-level pricing with sustained use discounts baked in. Azure mixes meters and categories in ways that map to neither.

FOCUS gives you one schema. This pipeline does the mapping so your dashboards and alerts don't need provider-specific logic.

## Service Categorization

The categorization engine covers core services across all four providers:

| Category | AWS | GCP | Azure | OCI |
|----------|-----|-----|-------|-----|
| Compute | EC2, Lambda, ECS, EKS, Fargate | Compute Engine, Cloud Functions, GKE, Cloud Run | Virtual Machines, Functions, AKS, Container Instances | Compute |
| Storage | S3, EBS, EFS, Glacier | Cloud Storage, Persistent Disk, Filestore | Blob Storage, Disk Storage, Files | Object Storage, Block Storage |
| Database | RDS, DynamoDB, ElastiCache, Redshift | Cloud SQL, Spanner, Bigtable, Firestore | SQL Database, Cosmos DB, Cache for Redis | Autonomous DB, MySQL, NoSQL |
| Networking | VPC, CloudFront, Route53, ELB | VPC, Cloud CDN, Cloud DNS, Load Balancing | Virtual Network, CDN, DNS, Load Balancer | VCN, Load Balancing |

Anything that doesn't match a rule falls into "Other". The mapping lives in a dict, easy to extend when new services launch.

## Running It

```python
from finops_etl import MultiCloudFinOpsETL

config = {
    "aws": {
        "bucket": "your-cur-bucket",
        "prefix": "cur-reports/",
        "region": "us-east-1"
    },
    "gcp": {
        "project_id": "your-project",
        "dataset": "billing_export",
        "table": "gcp_billing_export_v1"
    },
    "warehouse": {
        "type": "bigquery",
        "project_id": "your-analytics-project",
        "dataset": "finops",
        "table": "unified_costs"
    }
}

etl = MultiCloudFinOpsETL(config)
etl.run()
```

Dependencies: `pandas`, `boto3`, `google-cloud-bigquery`, `azure-storage-blob`, `oci`. Install with:

```bash
pip install pandas boto3 google-cloud-bigquery azure-storage-blob oci
```

## Context

I built this after running FinOps across multiple engagements. At one consulting client, I cut their AWS bill from $11-12K/month to under $3K. At a fintech company, the reduction was over 30%, roughly $200K/year annualized. Every time, the first step was the same: get all the cost data into one place where you can compare it across providers.

This pipeline is that first step, generalized. The tool I wished existed when I was doing the analysis by hand across four different provider consoles.
