---
applications:
- name: scoring-engine-spark-tk
  command: bin/model-scoring.sh
  memory: 1G
  disk_quota: 1G
  timeout: 180
  instances: 1
  services:
    - hdfs-for-se
    - kerberos-for-atk
  env:
    VERSION: "20161014"
