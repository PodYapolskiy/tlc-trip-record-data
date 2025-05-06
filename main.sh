#!/bin/bash

# Run the big data pipeline
echo "Running Stage 1 of the pipeline - Preprocessing + PostgreSQL + Sqoop"
bash scripts/stage1.sh

echo "Running Stage 2 of the pipeline - Hive + Spark SQL"
bash scripts/stage2.sh

echo "Running Stage 3 of the pipeline - Spark ML"
bash scripts/stage3.sh

echo "Running Stage 4 of the pipeline - Streamlit"
bash scripts/stage4.sh

# Post-processing
echo "Running post-processing!"

# Check the quality of the codes
echo "The quality of scripts in 'scripts/' folder\n"
echo "::============================================::"
pylint scripts --ignore '.venv'

echo "Done testing the pipeline!"
