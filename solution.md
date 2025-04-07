# ⚡️ Energy Market Price Analytics Pipeline – AWS Glue + CDK

## 📐 Architecture Overview

This solution implements a scalable and production-ready ETL pipeline for processing German energy market price data. It leverages:

- **AWS Glue** for ETL processing and workflow orchestration  
- **Amazon S3** for data lake storage (raw → refined → gold)  
- **AWS Glue Data Catalog + Athena** for metadata management and querying  
- **CloudWatch & SNS** for monitoring and alerting  
- **AWS CDK (Python)** for infrastructure as code

---

## ✅ Key Features

- ✅ Accurate handling of **Europe/Berlin timezone** and **daylight saving time**
- ✅ Implementation of **Slowly Changing Dimension (SCD) Type 2** logic  
- ✅ Glue **Workflow and Trigger orchestration**
- ✅ Partitioned Parquet output optimized for **Athena querying**
- ✅ Built-in **data quality validation** with reporting

---

## 📌 Assumptions

1. Market price data is fetched via an API and delivered as monthly CSV files into the landing S3 bucket.  
   - A sample file can be found at:  
     `energy_pipeline_cdk/scripts/landing/data/daa_test_data.csv`  
   - Files include an `ingestion_time` column.
   
2. Data is **batch-based** for now. If the source evolves to **streaming**, AWS Kinesis should replace Glue for ingestion.

3. **Source-side CDC** is implicit — i.e., changed records arrive only in future files, not by updating the same file.

4. For robust CDC/SCD2 support, the source system should eventually provide an **explicit column indicating current record** status.

5. **CI/CD setup** using GitHub Actions is included in the project. However, due to time constraints, full CI/CD automation and validation pipelines can be further enhanced in later phases.

---

## 🗂️ Repository Structure

```
├── config/                        # JSON/YAML schema or job config
├── deployment/                   # GitHub Actions CI/CD workflow
│   └── deploy.yml
├── energy_pipeline_cdk/          # Main CDK app and orchestration logic
│   ├── app.py
│   ├── checkpoints/              # Checkpoints for raw ingestion
│   ├── scripts/                  # All Glue jobs (ETL logic)
│   │   ├── landing/ raw/ refined/ gold/ data_quality/
├── tests/                        # Unit test modules for Glue and CDK
│   ├── unit_test_cdk/
│   ├── unit_test_scripts/
├── daa_test_data.csv             # Sample test data (monthly)
├── requirements.txt              # Python dependencies
├── .gitignore                    # Ignore common junk (.venv, __pycache__)
├── Makefile                      # Developer helper commands
├── README.md
├── solution.md                   # Detailed architectural explanation
├── source.bat                    # Activate local virtualenv
├── cdk.json / cdk.context.json   # CDK app context config
```

---

## 🚀 Getting Started

### 🧰 Prerequisites

- AWS CLI configured  
- Python 3.9+  
- AWS CDK v2 installed (`npm install -g aws-cdk`)  

### ⚙️ Deployment

```bash
pip install -r requirements.txt
cdk bootstrap
cdk deploy --all
```

---

## 🔐 Access Information

- **AWS Console Login**:  
  https://230908437522.signin.aws.amazon.com/console  
- **Username**: `Entrix_user`  
- **Password**: `Entrix_user`  
*(for evaluation purposes only; rotate credentials in production)*

- **Athena query result location**:  
  `s3://energypipelinebasestack-apilandingbucket509a20aa-yiycekc5mb5k/athena_dev_data/`

---

## 📄 Documentation & Diagrams

For full architecture diagrams and implementation planning, refer to:

📄 Confluence: [View Documentation](https://rinilaha1.atlassian.net/wiki/x/oAAB)

---

## 👩‍💻 Author

**Rini Laha**  
Senior Data Engineer  
📧 rinilaha@gmail.com
https://www.linkedin.com/in/rini-laha/