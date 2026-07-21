# ADR-0003: Divisão de responsabilidades entre Glue Job e dbt

## Status
Aceito

## Contexto
O bcb-warehouse introduz dois motores de transformação na stack:

- **AWS Glue Job (PySpark):** processamento distribuído, nativo AWS, adequado para
  transformações de baixo nível com tipagem estrita, deduplicação e limpeza avançada
- **dbt Core (SQL + Jinja):** modelagem dimensional em SQL, contratos de schema,
  testes declarativos, lineage automático e documentação

Esses dois motores podem se sobrepor em responsabilidades. Sem uma fronteira clara,
o risco é de duplicação de lógica, inconsistência entre camadas e dificuldade de
manutenção: a mesma regra de negócio implementada duas vezes (em PySpark e em SQL)
torna-se uma fonte de divergência.

A questao central é: **qual motor faz o quê, e onde exatamente está a fronteira?**

Tres opções de divisao foram consideradas:

| Opção | Glue Job | dbt |
|---|---|---|
| A — Glue faz tudo até o mart | Limpeza + tipagem + dedup + modelagem dimensional | Apenas testes e documentação |
| B — Fronteira na staging Iceberg | Limpeza + tipagem + dedup + escrita Iceberg staging | Modelagem: staging → intermediate → mart |
| C — dbt faz tudo via SQL | Apenas leitura raw → escrita staging Parquet | Dedup + limpeza + modelagem |

## Decisão
**Opção B — Fronteira na staging Iceberg.**

O Glue Job é responsável por **limpeza de baixo nível e preparação dos dados**:

1. Leitura do Parquet da camada `bcb-pipeline-staging` (output do Projeto 1)
2. Casting explícito de tipos (data como `DATE`, valor como `DECIMAL(18,6)`)
3. Deduplicação por chave natural (`serie_id`, `data`)
4. Validação de intervalos (valores fora de range marcados como nulos, nao descartados)
5. Escrita em Iceberg no prefixo `s3://bcb-warehouse/warehouse/intermediate/stg_{serie}/`

O dbt é responsável por **modelagem dimensional e contratos de qualidade**:

1. Camada `staging/` (modelos `stg_*`): renomear colunas, aplicar aliases, nao transformar dados
2. Camada `intermediate/` (modelos `int_*`): joins entre series, cálculo de métricas derivadas
3. Camada `mart/` (modelos `fct_*`, `dim_*`): star schema final, pronto para consumo

Representacao do fluxo:

```
bcb-pipeline-staging (Parquet)
       |
       v
  [Glue Job PySpark]
  limpeza + tipagem + dedup
       |
       v
bcb-warehouse/warehouse/intermediate/stg_{serie}/ (Iceberg)
       |
       v
  [dbt — camada staging]
  stg_usd_brl, stg_selic, stg_ipca
       |
       v
  [dbt — camada intermediate]
  int_indicadores_economicos
       |
       v
  [dbt — camada mart]
  fct_cotacoes_diarias, dim_serie, dim_data
```

## Consequências

### Positivas
- Fronteira clara e única: tudo abaixo da linha `stg_*` Iceberg é responsabilidade do
  Glue Job; tudo acima é responsabilidade do dbt
- PySpark lida com o que SQL trata mal: casting de datas com múltiplos formatos,
  deduplicação com window functions sobre datasets que podem crescer, validação de
  ranges com lógica imperativa
- dbt lida com o que SQL trata bem: joins, agregações, aliases, testes declarativos
  (`not_null`, `unique`, `accepted_values`), documentação de colunas via `schema.yml`
- Lineage completo no dbt: os modelos `stg_*` são a fonte de verdade declarada no dbt;
  o Glue Job é tratado como um produtor externo
- Reprocessamento independente: o Glue Job pode ser reexecutado sem afetar os modelos
  dbt enquanto o schema Iceberg permanecer estável

### Negativas / Trade-offs
- Dois motores para entender, depurar e manter — overhead cognitivo real
- Se uma regra de negócio precisar mudar (ex: recalcular IPCA por um critério diferente),
  é necessário avaliar em qual camada ela vive antes de alterar
- A camada `stg_*` do Glue Job e a camada `staging/` do dbt têm nomes similares mas
  responsabilidades distintas — pode causar confusão sem documentação clara (mitigado
  por esta ADR e pelo CLAUDE.md)
- O Glue Job precisa conhecer o schema Iceberg esperado pelo dbt — contrato implícito
  que deve ser mantido sincronizado manualmente

## Alternativas consideradas
- **Opção A (Glue faz tudo)**: elimina o dbt do fluxo de transformacao, mas perde os
  benefícios de testes declarativos, lineage automático e documentação de schema do dbt —
  rejeitada porque o objetivo do Projeto 2 é demonstrar dbt como ferramenta de modelagem
- **Opção C (dbt faz tudo via SQL)**: o dbt-athena pode ler diretamente o Parquet da staging
  e materializar em Iceberg, eliminando o Glue Job da cadeia de transformacao — rejeitada
  porque o Glue Job com PySpark é um requisito explícito do roadmap (para demonstrar
  domínio de Spark) e porque deduplicação e tipagem avancada sao mais robustas em PySpark
  do que em SQL puro sobre Athena

## Revisão
Elaborado por: Claude (Agente IA) — arquiteto-dados
Data/hora: 2026-07-18 09:00 BRT

## Aprovação
Aprovado por: Lucas de Araújo
Data/hora: 2026-07-20 12:01 BRT

## Errata — 2026-07-21

**Path S3 e nome de catálogo das tabelas intermediárias do Glue Job.**

O fluxo documentado na seção Decisão indicava o path:

```
bcb-warehouse/warehouse/intermediate/stg_{serie}/ (Iceberg)
```

O prefixo `stg_` **não é usado** nos nomes de tabela do Glue Data Catalog para as tabelas
escritas pelo Glue Job. O nome no catálogo é `usd_brl`, `selic`, `ipca` — sem prefixo.
Motivo: o Athena não permite VIEW e TABLE com o mesmo nome no mesmo database; as views
dbt da camada staging são nomeadas `stg_usd_brl`, `stg_selic`, `stg_ipca`, então as
tabelas Iceberg do Glue Job não podem usar o mesmo prefixo.

O path S3 físico segue o `warehouse_location` configurado no Glue Data Catalog + nome da
tabela, resultando em:

```
s3://bcb-warehouse/warehouse/intermediate/usd_brl/   (Iceberg — Glue Job)
s3://bcb-warehouse/warehouse/intermediate/selic/
s3://bcb-warehouse/warehouse/intermediate/ipca/
```

As views dbt `stg_usd_brl`, `stg_selic`, `stg_ipca` lêem essas tabelas Iceberg e aplicam
aliases de colunas — sem materialização física adicional.

Corrigido por: Claude (Agente IA) — desenvolvedor-dados
Aprovação: Lucas de Araújo — 2026-07-21
