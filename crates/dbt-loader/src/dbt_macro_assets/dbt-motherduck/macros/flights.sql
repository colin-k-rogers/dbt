{#
  Name of the MotherDuck Flight that runs a Python model. Flight names are
  unique per MotherDuck user, so override `motherduck__flight_name` when the
  default would collide, for example two targets sharing a database and schema.
#}

{% macro motherduck__flight_name(parsed_model) -%}
  {%- set parts = ['dbt'] -%}
  {%- for key in ['package_name', 'database', 'schema'] -%}
    {%- if parsed_model.get(key) -%}
      {%- do parts.append(parsed_model.get(key)) -%}
    {%- endif -%}
  {%- endfor -%}
  {%- set identifier = parsed_model.get('alias') or parsed_model.get('name') -%}
  {%- if identifier -%}
    {%- do parts.append(identifier) -%}
  {%- endif -%}
  {{ return(parts | join('-')) }}
{%- endmacro %}
