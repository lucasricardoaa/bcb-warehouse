# ADR-0005: Orquestração — DAG estendida vs DAG separada para o warehouse

## Status
Aceito

## Contexto
O bcb-pipeline (Projeto 1) possui uma DAG `bcb_ingestion` que orquestra:

```
extract_from_bcb_api >> write_raw_to_s3 >> invoke_lambda_raw_to_staging >> run_athena_query
```

O bcb-warehouse adiciona etapas de transformação que devem ser orquestradas:

```
[etapas existentes] >> run_glue_job >> dbt_run >> dbt_test
```

A decisão é: **essas novas etapas devem ser adicionadas à DAG existente do bcb-pipeline,
ou uma nova DAG separada deve ser criada para o warehouse?**

Duas abordagens foram consideradas:

**Opção A — DAG única estendida** (`bcb_ingestion`): as etapas do Glue Job e dbt são
adicionadas como tasks à DAG existente, formando um pipeline linear de ponta a ponta.

**Opção B — DAG separada** (`bcb_warehouse`): uma nova DAG independente, acionada por
sensor ou dataset trigger após a conclusao da DAG de ingestão.

Critérios de decisão:

| Critério | DAG única (A) | DAG separada (B) |
|---|---|---|
| Visibilidade do fluxo ponta a ponta | Alta — um único grafo | Baixa — dois grafos |
| Acoplamento entre projetos | Alto | Baixo |
| Impacto de falha na ingestão | Bloqueia transformacao (desejável) | Depende do trigger |
| Re-execucao seletiva | Requer clear seletivo | Trigger manual da DAG warehouse |
| Ciclos de deploy independentes | Nao | Sim |
| Complexidade operacional | Baixa | Media |

## Decisão
**Opção B — DAG separada `bcb_warehouse`**, acionada via **Airflow Dataset** quando a
DAG `bcb_ingestion` atualizar o dataset de staging.

A DAG `bcb_warehouse` possui a seguinte estrutura de tasks:

```
wait_for_staging_dataset >> run_glue_job >> dbt_run_staging >> dbt_run_intermediate
  >> dbt_run_mart >> dbt_test
```

A manutenção Iceberg (OPTIMIZE + VACUUM) é responsabilidade de uma **DAG dedicada
`bcb_warehouse_maintenance`**, com schedule mensal fixo no dia 15. Essa separação foi
decidida na ADR-0008 — a manutenção não pode ser integrada à `bcb_warehouse` porque
o timing do Dataset trigger (segundas-feiras) inviabiliza o VACUUM efetivo no mesmo ciclo.

Quadro completo das DAGs do projeto:

| DAG | Projeto | Trigger | Responsabilidade |
|---|---|---|---|
| `bcb_ingestion` | bcb-pipeline | Schedule semanal | Ingestão API BCB → S3 staging |
| `bcb_warehouse` | bcb-warehouse | Dataset event | Transformação Glue + modelagem dbt |
| `bcb_warehouse_maintenance` | bcb-warehouse | Schedule dia 15/mês | OPTIMIZE + VACUUM nas tabelas Iceberg |

O Dataset Airflow é declarado com o URN do prefixo S3 de staging:

```python
STAGING_DATASET = Dataset("s3://bcb-pipeline-staging/bcb/")
```

A DAG `bcb_ingestion` declara que atualiza esse dataset ao final da task
`invoke_lambda_raw_to_staging`. A DAG `bcb_warehouse` declara que depende desse dataset
para iniciar.

## Consequências

### Positivas
- Ciclos de deploy independentes: o bcb-warehouse pode ser atualizado sem tocar na DAG
  de ingestão do bcb-pipeline — os dois projetos evoluem em ritmos distintos
- Re-execucao isolada: se um modelo dbt falhar, reexecutar apenas a DAG `bcb_warehouse`
  sem reingerir dados da API do BCB
- Falha na ingestão bloqueia automaticamente o warehouse via Dataset dependency —
  sem necessidade de sensores ad hoc ou condicionais na DAG
- Airflow Datasets é o mecanismo recomendado desde a versao 2.4 para dependências entre
  DAGs — demonstra conhecimento do recurso nativo
- Separacao clara de responsabilidades entre projetos no portfólio

### Negativas / Trade-offs
- Visibilidade do fluxo ponta a ponta requer navegar entre duas DAGs no Airflow UI —
  nao há um grafo único que mostre ingestão + transformacao
- Dataset URN baseado em prefixo S3 é uma convenção sem validação real — o Airflow
  não verifica se o prefixo foi modificado no S3; apenas registra que a task declarou
  o outlet. Se o `outlets` for removido da DAG `bcb_ingestion` por descuido, ou se
  o URN for alterado em apenas um dos lados, a DAG `bcb_warehouse` deixa de disparar
  sem emitir nenhum erro ou alerta — falha silenciosa. Mitigação: manter o URN
  `"s3://bcb-pipeline-staging/bcb/"` como constante compartilhada e incluir um
  monitor de SLA na DAG `bcb_warehouse`
- Airflow Datasets nao suportam parametrizacao por data_interval — a DAG warehouse
  processa o lote mais recente disponível, sem garantia de correlacao com a janela
  exata da ingestão (aceitável para este volume e frequência)
- Dois arquivos de DAG para manter em vez de um

## Alternativas consideradas
- **Opção A (DAG única)**: seria mais simples para demonstrar o fluxo completo, mas acopla
  o ciclo de vida do warehouse ao da ingestão — qualquer mudança na DAG `bcb_ingestion`
  exige testes das etapas de warehouse e vice-versa. Rejeitada porque os dois projetos
  devem poder evoluir independentemente, o que é uma propriedade desejável a demonstrar
  em portfólio
- **ExternalTaskSensor**: alternativa ao Dataset — a DAG warehouse usa um sensor que aguarda
  a task `invoke_lambda_raw_to_staging` da DAG `bcb_ingestion` completar. Rejeitada porque
  ExternalTaskSensor acopla a DAG warehouse ao nome específico de tasks da DAG de ingestão,
  criando dependência frágil. Datasets é o padrao moderno e correto para este caso

## Revisão
Elaborado por: Claude (Agente IA) — arquiteto-dados
Data/hora: 2026-07-18 09:00 BRT

## Aprovação
Aprovado por: Lucas de Araújo
Data/hora: 2026-07-20 12:47 BRT

## Errata — 2026-07-21

**`wait_for_staging_dataset` não existe como task no grafo da DAG.**

A estrutura de tasks documentada na seção Decisão incluía `wait_for_staging_dataset`
como primeiro nó:

```
wait_for_staging_dataset >> run_glue_job >> dbt_run_staging >> ...
```

No Airflow 2.4+ com Dataset scheduling, a dependência de Dataset é declarada no
decorador `@dag(schedule=[Dataset(...)])` e processada pelo scheduler — não existe
como task explícita no grafo. A DAG `bcb_warehouse` não contém nenhum `DatasetSensor`
nem task equivalente; o scheduler dispara o DAG Run automaticamente quando o Dataset
é atualizado pela DAG produtora.

A sequência real de tasks no grafo é:

```
run_glue_job >> dbt_run_staging >> dbt_run_intermediate >> dbt_run_mart >> dbt_test
```

A dependência de Dataset aparece na Airflow UI como metadado do DAG (aba "Datasets"),
não como nó no Task Graph.

Corrigido por: Claude (Agente IA) — desenvolvedor-dados
Aprovação: Lucas de Araújo — 2026-07-21
