# `index.py` script

- [`index.py` script](#indexpy-script)
  - [Script Arguments](#script-arguments)
  - [Script Execution](#script-execution)
  - [Used Libraries](#used-libraries)
  - [Templates](#templates)


This script generates report from parquet files. It iterates over all parquet files in the provided directory, reads the parquet metadata for retrieving the number of rows in each file and file size, and calculates the total number of rows and total file size. After that, it generates the report in the specified format using Jinja2 template.

The script supports generating index in the following formats:
- html
- markdown
- typst

## Script Arguments

- `--source-dir` - source directory for dataset files
- `--output-format` - output format for the index, can be `html`, `markdown` or `typst`
- `--output-file` - output file path
- `--summary` - whether to include summary row with totals
- `--replace-root` - replace root for download links

## Script Execution

```bash
uv run index/generate-index.py \
    --source-dir data \
    --output-format html \
    --output-file index.html \
    --summary \
    --replace-root https://bigdata.inno.dartt0n.ru/ibd/
```

## Used Libraries

- [argparse](https://docs.python.org/3/library/argparse.html) - for parsing script arguments
- [jinja2](https://jinja.palletsprojects.com/) - for generating html report
- [pyarrow](https://arrow.apache.org/docs/python/) - for reading parquet files

## Templates

- [template.html](templates/template.html) - html template
- [template.md](templates/template.md) - markdown template
- [template.typst](templates/template.typst) - typst template