{#
  The few places where Snowflake and DuckDB SQL differ. Models call these
  macros, so the same models build on the local DuckDB warehouse and on
  Snowflake. default__ = DuckDB, snowflake__ = Snowflake.
#}

{# String value at a JSON path: json_str('raw', ['rates', 'USDXAU']) #}
{% macro json_str(column, path) -%}
    {{ return(adapter.dispatch('json_str')(column, path)) }}
{%- endmacro %}

{% macro default__json_str(column, path) -%}
    json_extract_string({{ column }}, '$.{% for key in path %}"{{ key }}"{% if not loop.last %}.{% endif %}{% endfor %}')
{%- endmacro %}

{% macro snowflake__json_str(column, path) -%}
    {{ column }}{% for key in path %}:"{{ key }}"{% endfor %}::string
{%- endmacro %}


{# JSON value (object/array) at a path, still JSON/VARIANT #}
{% macro json_get(column, path) -%}
    {{ return(adapter.dispatch('json_get')(column, path)) }}
{%- endmacro %}

{% macro default__json_get(column, path) -%}
    json_extract({{ column }}, '$.{% for key in path %}"{{ key }}"{% if not loop.last %}.{% endif %}{% endfor %}')
{%- endmacro %}

{% macro snowflake__json_get(column, path) -%}
    {{ column }}{% for key in path %}:"{{ key }}"{% endfor %}
{%- endmacro %}


{# One row per key of a JSON object, exposing <alias>.key and <alias>.value.
   Use after a comma in FROM: from src, {{ flatten_object('src.raw') }} as f #}
{% macro flatten_object(json_expr) -%}
    {{ return(adapter.dispatch('flatten_object')(json_expr)) }}
{%- endmacro %}

{% macro default__flatten_object(json_expr) -%}
    json_each({{ json_expr }})
{%- endmacro %}

{% macro snowflake__flatten_object(json_expr) -%}
    lateral flatten(input => {{ json_expr }})
{%- endmacro %}


{# Unix epoch seconds -> naive UTC timestamp #}
{% macro epoch_to_timestamp(expr) -%}
    {{ return(adapter.dispatch('epoch_to_timestamp')(expr)) }}
{%- endmacro %}

{% macro default__epoch_to_timestamp(expr) -%}
    make_timestamp(cast({{ expr }} as bigint) * 1000000)
{%- endmacro %}

{% macro snowflake__epoch_to_timestamp(expr) -%}
    to_timestamp_ntz({{ expr }})
{%- endmacro %}


{# Left ASOF join: attach the latest right-hand row whose match_condition
   holds (e.g. the most recent FX rate on or before a transaction date).
   Rows with no match keep NULLs, like a LEFT JOIN. #}
{% macro asof_left_join(relation, alias, match_condition, on_condition) -%}
    {{ return(adapter.dispatch('asof_left_join')(relation, alias, match_condition, on_condition)) }}
{%- endmacro %}

{% macro default__asof_left_join(relation, alias, match_condition, on_condition) -%}
    asof left join {{ relation }} {{ alias }}
        on {{ on_condition }} and {{ match_condition }}
{%- endmacro %}

{% macro snowflake__asof_left_join(relation, alias, match_condition, on_condition) -%}
    asof join {{ relation }} {{ alias }}
        match_condition ({{ match_condition }})
        on {{ on_condition }}
{%- endmacro %}
