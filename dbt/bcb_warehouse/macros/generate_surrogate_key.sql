{#
  Gera surrogate key determinística via SHA-256 truncado (32 hex chars = 128 bits).

  Motivação: SHA-256 é mais robusto que MD5 (padrão do dbt_utils) para garantir
  ausência de colisões em reprocessamentos. Truncamento para 32 chars preserva
  a propriedade de unicidade para o volume de dados do bcb-warehouse (ADR-0004).

  Compatibilidade: Athena via funções nativas sha256() + to_hex() + to_utf8().

  Uso:
    {{ generate_surrogate_key(['serie_id']) }}
    {{ generate_surrogate_key(['serie_id', 'data']) }}

  Esta macro sobrepõe dbt_utils.generate_surrogate_key no escopo deste projeto.
#}

{% macro generate_surrogate_key(field_list) %}

    {%- set null_placeholder = "_sk_null_" -%}
    {%- set coalesced = [] -%}

    {%- for field in field_list -%}
        {%- set _ = coalesced.append(
            "coalesce(cast(" ~ field ~ " as varchar), '" ~ null_placeholder ~ "')"
        ) -%}
    {%- endfor -%}

    substr(
        to_hex(
            sha256(
                to_utf8(
                    {{ coalesced | join(" || '|' || ") }}
                )
            )
        ),
        1, 32
    )

{% endmacro %}
