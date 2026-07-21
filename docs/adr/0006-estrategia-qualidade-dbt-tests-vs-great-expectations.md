# ADR-0006: Estratégia de qualidade de dados — dbt tests vs Great Expectations

## Status
Aceito

## Contexto
O bcb-warehouse requer uma camada de qualidade de dados que valide:

1. Integridade referencial entre fatos e dimensões
2. Ausência de nulos em colunas obrigatórias
3. Unicidade de surrogate keys
4. Ranges válidos de valores (ex: taxa Selic entre 0% e 100%, câmbio maior que zero)
5. Completude temporal (nao há buracos inesperados nas séries)

Duas ferramentas foram consideradas para este papel:

**dbt tests (nativos + dbt-utils):** testes declarativos em YAML executados pelo próprio
dbt como parte do `dbt test`. Integrados ao lineage do dbt. Output: pass/fail por modelo.

**Great Expectations (GE):** framework de qualidade independente com expectativas
parametrizadas, Data Docs (relatórios HTML), integracao com múltiplas fontes de dados.

A stack do roadmap menciona Great Expectations como opcional.

Critérios de decisão:

| Critério | dbt tests | Great Expectations |
|---|---|---|
| Integração com dbt | Nativa | Via callback ou tarefa separada |
| Curva de aprendizado | Baixa | Alta |
| Visibilidade no Airflow | Task `dbt_test` — pass/fail | Task GE separada + Data Docs |
| Tipos de teste cobertos | Genéricos + custom SQL | Qualquer expectativa |
| Overhead de configuração | YAML no schema.yml | Suite de expectativas + Checkpoint |
| Valor de portfólio | Fundamental (toda vaga dbt exige) | Diferencial (adicional) |
| Risco de over-engineering | Baixo | Alto para este volume |

## Decisão
**dbt tests como camada primária e mandatória de qualidade**, com o seguinte conjunto
mínimo de testes por camada:

Camada `staging/` (modelos `stg_*`):
- `not_null` em todas as colunas da chave natural
- `unique` na surrogate key de cada modelo
- `accepted_values` em `periodicidade` (dim_serie)

Camada `intermediate/` (modelos `int_*`):
- `not_null` em colunas de FK
- `relationships` verificando que FKs existem nas dimensões

Camada `mart/` (modelos `fct_*`, `dim_*`):
- `not_null` em todas as colunas not-null do schema
- `unique` em surrogate keys
- `dbt-utils: expression_is_true` para ranges de valores (ex: `valor > 0` para câmbio e Selic)
- `dbt-utils: recency` sobre `data_processamento` para verificar que o pipeline rodou
  dentro do intervalo esperado — parâmetros por tabela:

  | Tabela | datepart | interval | Justificativa |
  |---|---|---|---|
  | `fct_cotacoes_diarias` | day | 10 | Pipeline semanal (segunda-feira) + fins de semana + feriados + lag de processamento |
  | `fct_indicadores_mensais` | day | 45 | IPCA divulgado entre dias 8–12 do mês seguinte; pipeline semanal captura em até 7 dias após divulgação |

**Great Expectations nao será implementado neste projeto.** A decisao é explícita:
o risco de over-engineering para um volume de dados de KB/semana supera o benefício
de demonstrar a ferramenta. A energia é melhor investida em testes dbt bem escritos
e documentados, que sao exigidos em praticamente todas as vagas com dbt.

Se o portfólio precisar demonstrar Great Expectations, o contexto adequado é um
projeto com múltiplas fontes, volumes maiores e expectativas complexas de distribuicao
estatística — nao séries temporais de três indicadores.

## Consequências

### Positivas
- Testes integrados ao grafo dbt — `dbt test` na DAG falha a task e bloqueia o pipeline
  se qualquer teste falhar, sem necessidade de tarefa adicional
- Schema.yml com testes documenta o contrato de cada modelo — valor de portfólio direto
- dbt-utils expande os testes genéricos sem framework adicional
- Zero overhead de configuração de suites, checkpoints e Data Docs

### Negativas / Trade-offs
- Sem Great Expectations, nao há relatórios HTML de qualidade gerados automaticamente —
  a visibilidade de falhas é limitada aos logs do Airflow e ao output do `dbt test`
- Testes dbt sao orientados a linhas/colunas; nao cobrem bem padrões estatísticos
  (ex: detectar anomalias de distribuicao ou desvios de Z-score) — limitação aceitável
  para séries BCB cujos ranges esperados sao conhecidos e estáveis
- Se o projeto evoluir para incluir validacoes estatísticas, a ausência de GE exigirá
  adicionar o framework posteriormente sem retrocompatibilidade trivial

## Alternativas consideradas
- **Great Expectations como camada primária**: cobrirá os mesmos casos que dbt tests com
  mais verbosidade de configuração e sem integração nativa ao lineage dbt — rejeitado
  por over-engineering para o volume e pela redundância com dbt tests
- **Ambos (dbt tests + GE em paralelo)**: duplica esforço de configuração e manutenção
  sem cobertura adicional real para os dados do BCB — rejeitado

## Revisão
Elaborado por: Claude (Agente IA) — arquiteto-dados
Data/hora: 2026-07-18 09:00 BRT

## Aprovação
Aprovado por: Lucas de Araújo
Data/hora: 2026-07-20 12:59 BRT
