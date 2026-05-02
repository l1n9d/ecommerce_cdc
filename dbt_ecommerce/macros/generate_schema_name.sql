{# 
   Custom macro to override dbt's default schema name generation.
   
   Default behavior: concatenates profile schema + model schema config:
       profile.schema = "STAGING" + model +schema = "staging" 
       -> final schema "STAGING_staging"
   
   This macro: uses the model's +schema config directly when provided,
   falls back to profile schema only when no override is given.
   
   With this in place:
       models in models/staging/ with +schema: STAGING -> ECOMMERCE_CDC.STAGING
       models in models/marts/ with +schema: MARTS    -> ECOMMERCE_CDC.MARTS
#}

{% macro generate_schema_name(custom_schema_name, node) -%}
    {%- set default_schema = target.schema -%}
    {%- if custom_schema_name is none -%}
        {{ default_schema }}
    {%- else -%}
        {{ custom_schema_name | trim }}
    {%- endif -%}
{%- endmacro %}
