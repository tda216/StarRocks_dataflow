{% materialization starrocks_materialized_view, adapter='starrocks' %}
  {%- set distributed_by = config.get('distributed_by', none) -%}
  {%- set buckets = config.get('buckets', 4) -%}

  {{ run_hooks(pre_hooks) }}

  {% call statement('drop_materialized_view') -%}
    DROP MATERIALIZED VIEW IF EXISTS {{ this }}
  {%- endcall %}

  {% call statement('drop_view') -%}
    DROP VIEW IF EXISTS {{ this }}
  {%- endcall %}

  {% call statement('drop_table') -%}
    DROP TABLE IF EXISTS {{ this }}
  {%- endcall %}

  {% call statement('main') -%}
    CREATE MATERIALIZED VIEW {{ this }}
    {%- if distributed_by %}
    DISTRIBUTED BY HASH({{ distributed_by }}) BUCKETS {{ buckets }}
    {%- endif %}
    REFRESH MANUAL
    AS
    {{ sql }}
  {%- endcall %}

  {% call statement('refresh') -%}
    REFRESH MATERIALIZED VIEW {{ this }} WITH SYNC MODE
  {%- endcall %}

  {{ run_hooks(post_hooks) }}

  {{ return({'relations': [this]}) }}
{% endmaterialization %}
