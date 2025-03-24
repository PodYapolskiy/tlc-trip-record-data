# data files

| year | month | size (mb) | rows | file |
| ---- | ----- | --------- | ---- | ---- |
{% for file in files %}|{{ file.year }}|{{ file.month }}|{{ file.size }}|{{ file.row_count }}|[{{ file.file_name }}]({{ file.file_path }})|
{% endfor %}||||||
{% if summary %}|**{{ summary.title }}**||**{{ summary.total_size }}**|**{{ summary.total_rows }}**||
{% endif %}
