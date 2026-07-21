# ADR-0002: Estrutura de camadas S3 do bcb-warehouse

## Status
Aceito

## Contexto
O bcb-pipeline (Projeto 1) estabeleceu três buckets dedicados por camada (ADR-0001 do
Projeto 1): `raw`, `staging` e `analytics`. O bcb-warehouse introduz novas camadas de
transformação que precisam de endereçamento S3 claro.

O dbt com adapter Athena requer um bucket de saída para escrever os arquivos Iceberg das
tabelas materializadas. O Glue Job também precisa de um bucket para:
- Scripts do job
- Arquivos temporários de shuffle do Spark
- Logs de execução

Duas questões precisam ser decididas:

**Questao 1 — Reutilizar o bucket `bcb-pipeline-analytics` ou criar um novo bucket
`bcb-warehouse`?**

O bucket `bcb-pipeline-analytics` foi criado para a visao consolidada do Projeto 1.
Reutilizá-lo para os marts do warehouse misturaria outputs de dois projetos distintos
no mesmo bucket, complicando IAM, lifecycle rules e atribuição de custo.

**Questao 2 — Como organizar os prefixos dentro do bucket warehouse para separar
outputs do Glue Job, tabelas dbt e arquivos técnicos do Iceberg?**

## Decisão

**Questao 1 — Novo bucket dedicado `bcb-warehouse`** para todos os outputs do Projeto 2.
O bucket `bcb-pipeline-analytics` permanece inalterado, mantendo o contrato do Projeto 1.

**Questao 2 — Organização por responsabilidade dentro do bucket `bcb-warehouse`:**

```
s3://bcb-warehouse/
  warehouse/
    intermediate/
      {model_name}/           # tabelas intermediárias dbt (Iceberg)
    mart/
      {model_name}/           # tabelas mart dbt — star schema (Iceberg)
  glue/
    scripts/                  # script PySpark do Glue Job
    temp/                     # diretório temporário do Spark (shuffle, spill)
    logs/                     # logs de execucao do Glue Job
```

O bucket de staging do Athena (resultados de queries `SELECT`) usa um bucket separado
já existente ou um prefixo dedicado no `bcb-warehouse`:

```
s3://bcb-warehouse/
  athena-results/             # resultados de queries Athena (TTL 7 dias)
```

Buckets envolvidos no bcb-warehouse:

| Bucket | Proprietário | Papel |
|---|---|---|
| `bcb-pipeline-raw` | bcb-pipeline | Fonte — JSON bruto (somente leitura pelo warehouse) |
| `bcb-pipeline-staging` | bcb-pipeline | Fonte — Parquet limpo (somente leitura pelo warehouse) |
| `bcb-warehouse` | bcb-warehouse | Destino — Iceberg + artefatos Glue + resultados Athena |

## Consequências

### Positivas
- Isolamento físico entre projetos: custo, lifecycle e IAM do warehouse são independentes
  do pipeline de ingestão
- Prefixos `warehouse/intermediate/` e `warehouse/mart/` refletem diretamente a
  nomenclatura das camadas dbt — navegação intuitiva no console S3
- Glue Job tem prefixo dedicado para scripts e temp, sem risco de interferência
  com os dados analíticos
- Um único bucket para provisionar no Terraform do Projeto 3

### Negativas / Trade-offs
- Quatro buckets no total na conta (`raw`, `staging`, o antigo `analytics` do Projeto 1,
  e `warehouse`) — overhead operacional baixo, mas real
- A convencao `warehouse/mart/{model_name}/` pressupoe que os nomes dos modelos dbt são
  estáveis — renomear um modelo requer migração do prefixo S3 ou limpeza manual
- `athena-results/` no mesmo bucket que dados analíticos é uma convencao prática mas exige
  lifecycle rule separada (TTL curto para resultados, retencao longa para dados)

## Alternativas consideradas
- **Reutilizar `bcb-pipeline-analytics`**: elimina um bucket, mas mistura outputs de dois
  projetos com ciclos de vida e equipes (simuladas) distintas — rejeitado por violar o
  princípio de separação de responsabilidades estabelecido no ADR-0001 do Projeto 1
- **Bucket separado por camada dbt** (`bcb-warehouse-intermediate`, `bcb-warehouse-mart`):
  granularidade excessiva para o volume e complexidade deste projeto — rejeitado porque
  o benefício de IAM por camada não compensa o overhead de gerenciar três buckets adicionais

## Revisão
Elaborado por: Claude (Agente IA) — arquiteto-dados
Data/hora: 2026-07-18 09:00 BRT

## Aprovação
Aprovado por: Lucas de Araújo
Data/hora: 2026-07-20 12:01 BRT
