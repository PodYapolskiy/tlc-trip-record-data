= data files

#table(
  columns: (auto, auto, auto, auto, auto),
  inset: 10pt,
  align: horizon,
  table.header(
    [*year*], [*month*], [*size (mb)*], [*rows*], [*file*],
  ),
  {% for file in files %}
  [{{ file.year }}],
  [{{ file.month }}],
  [{{ file.size }}],
  [{{ file.row_count }}],
  [#link("{{ file.file_path }}")[{{ file.file_name }}]],
  {% endfor %}

  {% if summary %}
  table.footer(
    repeat: false,
    [*{{ summary.title }}*], [], [*{{ summary.total_size }}*], [*{{ summary.total_rows }}*], [],
  )
  {% endif %}
)
