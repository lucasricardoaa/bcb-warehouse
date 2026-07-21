# ADR-0001: Formato de armazenamento na camada warehouse — Apache Iceberg vs Parquet plano

## Status
Aceito

## Contexto
O bcb-pipeline (Projeto 1) produz dados na camada staging em Parquet/Snappy com particionamento
Hive-style (`year=YYYY/month=MM/`). O bcb-warehouse (Projeto 2) precisa de uma camada
warehouse — destino final das transformações dbt — com suporte a:

- Operações de upsert e merge (necessárias para reprocessamento de séries corrigidas pelo BCB)
- Schema evolution sem reescrita de dados históricos
- Time travel para auditoria e rollback de transformações incorretas
- Queries Athena com custo previsível e otimizado (partition pruning + file skipping)

O Parquet plano com particionamento Hive, padrão atual do pipeline, não suporta nativamente
merge/upsert nem time travel. Reprocessamentos exigem deleção e reescrita de partições inteiras.

Duas opções são viáveis no ecossistema AWS com Athena e Glue Data Catalog:

| Critério | Apache Iceberg | Delta Lake | Apache Hudi |
|---|---|---|---|
| Suporte nativo Athena | Sim (engine v3) | Sim (limitado) | Sim (limitado) |
| Suporte nativo Glue Data Catalog | Sim (catálogo nativo) | Via manifest | Via manifest |
| Suporte PySpark via Glue Job | Sim | Sim | Sim |
| Suporte dbt-athena-community | Sim (materialização iceberg) | Nao | Nao |
| Time travel | Sim | Sim | Sim |
| Merge/upsert | Sim | Sim | Sim |
| Adocao AWS | Alta (servico gerenciado) | Media | Baixa |

## Decisão
Adotar **Apache Iceberg** como formato de tabela na camada warehouse, gerenciado via
**AWS Glue Data Catalog** como metastore.

Escopo de aplicacao:
- Tabelas intermediárias e marts produzidas pelo dbt usam materialização `iceberg`
- A camada staging (entrada do Glue Job) permanece em Parquet/Snappy, sem alteração
  — alinhada com a decisao do bcb-pipeline (ADR-0002 do Projeto 1)
- O Glue Job lê Parquet da staging e escreve Iceberg na camada warehouse

Configuracao de referência para tabelas dbt:

```yaml
# dbt_project.yml
models:
  bcb_warehouse:
    +table_type: iceberg
    +format: parquet
    +write_compression: snappy
```

## Consequências

### Positivas
- Upsert e merge nativos — reprocessamentos nao exigem deleção e reescrita de particoes inteiras
- Schema evolution sem downtime — colunas podem ser adicionadas sem reescrever dados históricos
- Time travel até 7 dias (configuravel) — rollback de transformacoes incorretas sem restore manual
- Partition evolution — reorganizar particoes sem migração de dados
- Compatibilidade nativa com dbt-athena-community via materialização `iceberg`
- Suporte nativo no Athena engine v3 — sem configuracao adicional de conector
- AWS recomenda Iceberg para novos projetos de data lake — decisao alinhada com direcao do servico

### Negativas / Trade-offs
- Iceberg nao é suportado no Athena engine v2 — exige engine v3, que é o padrão atual
  mas requer verificacao no workgroup configurado
- Overhead de metadados: Iceberg gera arquivos de snapshot, manifest e manifest-list no S3
  além dos arquivos de dados — custo de armazenamento ligeiramente maior
- Compactacao de arquivos pequenos (small files problem) é responsabilidade do operador —
  necessário agendar OPTIMIZE + VACUUM periodicamente (estratégia definida na ADR-0008)
- Curva de aprendizado maior que Parquet plano para operadores menos familiarizados com
  formatos de tabela transacionais
- Delta Lake é amplamente adotado em ambientes Databricks — se o projeto migrar para
  Databricks, seria necessário avaliar conversao

## Alternativas consideradas
- **Parquet plano com particionamento Hive**: mantém consistência com a camada staging, zero
  curva de aprendizado adicional — rejeitada porque não suporta merge/upsert sem reescrita
  completa de partição, o que inviabiliza reprocessamentos granulares
- **Delta Lake**: suporte Athena limitado (requer manifest files, sem suporte nativo ao
  catálogo Glue como tabela transacional), sem suporte no dbt-athena-community — rejeitado
  por incompatibilidade com a stack definida
- **Apache Hudi**: adocao AWS menor, suporte Glue/Athena via integracoes parciais, sem
  suporte dbt-athena — rejeitado pelas mesmas razoes do Delta Lake

## Revisão
Elaborado por: Claude (Agente IA) — arquiteto-dados
Data/hora: 2026-07-18 09:00 BRT

## Errata — 2026-07-21

**Implementação de MERGE no Glue Job.**

A versão original do `bcb_staging_transform.py` usava `.createOrReplace()` na escrita Iceberg —
substituição completa da tabela a cada execução. Isso contradiz a principal justificativa desta
ADR (upsert/merge nativos para reprocessamentos granulares).

A função `write_iceberg` foi reescrita para implementar o comportamento correto:

- **Primeira execução** (tabela inexistente): cria a tabela via `.create()` com as propriedades
  Iceberg (`format-version: 2`, `write.parquet.compression-codec: snappy`)
- **Execuções subsequentes**: executa `MERGE INTO` com chave natural `(serie_id, data)` —
  atualiza `valor` apenas quando alterado (comparação null-safe via `<=>`) e insere linhas novas

```sql
MERGE INTO {table} t
USING {staging_view} s
ON t.serie_id = s.serie_id AND t.data = s.data
WHEN MATCHED AND NOT (t.valor <=> s.valor)
    THEN UPDATE SET t.valor = s.valor
WHEN NOT MATCHED
    THEN INSERT *
```

O parâmetro `--warehouse_bucket` foi removido de `getResolvedOptions`: o path S3 das tabelas
Iceberg é configurado no `warehouse_location` do Glue Data Catalog, não no script do job.

Corrigido por: Claude (Agente IA) — desenvolvedor-dados
Aprovação: Lucas de Araújo — 2026-07-21
