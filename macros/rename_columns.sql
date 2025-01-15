{% macro rename_columns(source_table, platform, source) %}
    {% set mappings_query %}
        select
            m.field_id,
            f.field_name as canonical_name
        from {{ source('ods', 'affiliate_mappings') }} m
        join {{ source('ods', 'affiliate_fields') }} f
        on m.field_id = f.field_id
        where m.field_platform = '{{ platform }}'
        and m.field_source = '{{ source }}'
    {% endset %}

    -- Step 1: Fetch mappings
    {% set mappings = run_query(mappings_query).to_dict() %}
    
    -- Step 2: Get columns from the source table
    {% set source_columns = adapter.get_columns_in_table(source_table) %}
    
    -- Step 3: Build SELECT statement
    {% set select_clauses = [] %}
    {% for column in source_columns %}
        {% if column.name in mappings %}
            {% do select_clauses.append(column.name ~ " as " ~ mappings[column.name]) %}
        {% else %}
            {% do select_clauses.append(column.name) %}
        {% endif %}
    {% endfor %}
    
    -- Return the SELECT statement
    {{ select_clauses | join(', ') }}
{% endmacro %}
