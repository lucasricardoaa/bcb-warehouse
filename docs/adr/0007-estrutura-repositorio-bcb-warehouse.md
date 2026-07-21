# ADR-0007: Estrutura de repositório do bcb-warehouse

## Status
Aceito

## Contexto
O bcb-warehouse é um projeto separado do bcb-pipeline, mas compartilha o mesmo ambiente
Airflow. A estrutura do repositório deve refletir as responsabilidades dos dois motores
de transformação (Glue Job e dbt) e tornar a navegação intuitiva para avaliadores técnicos.

O repositório do bcb-pipeline (Projeto 1) usa a seguinte convencao:

```
bcb-pipeline/
  dags/
  src/
  tests/
  docs/adr/
  docker/
```

O bcb-warehouse introduz dois novos domínios sem equivalente no Projeto 1:
- Um projeto dbt completo (com seus próprios diretórios `models/`, `tests/`, `seeds/`, etc.)
- Scripts PySpark para o Glue Job

A questao é: **onde cada domínio vive dentro do repositório?**

## Decisão
Estrutura adotada:

```
bcb-warehouse/
  dags/
    bcb_warehouse_dag.py           # DAG Airflow do warehouse (Dataset-triggered)
    bcb_warehouse_maintenance_dag.py  # DAG de manutenção Iceberg (schedule dia 15/mês)

  glue/
    jobs/
      bcb_staging_transform.py     # script PySpark principal do Glue Job
    requirements.txt               # dependências Python do Glue Job

  dbt/
    bcb_warehouse/                 # projeto dbt (diretório raiz do dbt)
      dbt_project.yml
      profiles.yml.example         # template — profiles.yml real em .gitignore
      packages.yml                 # dependências dbt (dbt-utils)
      models/
        staging/
          stg_usd_brl.sql
          stg_selic.sql
          stg_ipca.sql
          schema.yml               # testes e documentação da camada staging
        intermediate/
          int_indicadores_economicos.sql
          schema.yml
        mart/
          fct_cotacoes_diarias.sql
          fct_indicadores_mensais.sql
          dim_serie.sql
          dim_data.sql
          schema.yml
      seeds/
        dim_data_seed.csv          # calendário pré-gerado (2020-2030)
        feriados_nacionais.csv     # feriados nacionais brasileiros
        dim_serie.csv              # metadados das 3 séries BCB (ver ADR-0009)
      macros/
        generate_surrogate_key.sql # macro para hash determinístico de SK
      tests/
        generic/                   # testes genéricos customizados
      target/                      # output do dbt — em .gitignore

  tests/
    unit/
      glue/                        # testes unitários das transformacoes PySpark
    integration/                   # testes de integração (opcional)

  docs/
    adr/                           # ADRs deste projeto
    architecture/
      warehouse_architecture.md    # diagrama e descricao da arquitetura

  .env.example
  Makefile
  pyproject.toml                   # ruff, mypy, pytest config
  CLAUDE.md
```

Convencoes de nomenclatura dos modelos dbt:

| Prefixo | Camada | Exemplo |
|---|---|---|
| `stg_` | staging | `stg_usd_brl` |
| `int_` | intermediate | `int_indicadores_economicos` |
| `fct_` | mart — tabela fato | `fct_cotacoes_diarias` |
| `dim_` | mart — dimensao | `dim_serie`, `dim_data` |

O `profiles.yml` nao é versionado (contém credenciais Athena). O repositório versiona
apenas o `profiles.yml.example` com placeholders documentados.

## Consequências

### Positivas
- `glue/` e `dbt/` sao domínios distintos com raízes claras — nenhum arquivo PySpark
  dentro do projeto dbt e vice-versa
- Seguidor do padrao dbt de ter o projeto dentro de um subdiretório nomeado
  (`dbt/bcb_warehouse/`) permite múltiplos projetos dbt no mesmo repositório no futuro
- `seeds/dim_data_seed.csv` versionado no repositório documenta a dimensao calendário
  sem dependência de script externo
- Makefile centraliza comandos frequentes (`make dbt-run`, `make dbt-test`, `make glue-test`)

### Negativas / Trade-offs
- `dbt/bcb_warehouse/` é um nível de aninhamento a mais que o padrão de projetos dbt
  standalone (onde `dbt_project.yml` fica na raiz) — necessário ajustar `DBT_PROFILES_DIR`
  e `DBT_PROJECT_DIR` na DAG Airflow
- `seeds/dim_data_seed.csv` para 10 anos de calendário tem ~3.650 linhas — tamanho
  gerenciável, mas o arquivo deve ser regenerado se o intervalo histórico for expandido

## Alternativas consideradas
- **Projeto dbt na raiz do repositório**: `dbt_project.yml` diretamente em `bcb-warehouse/` —
  rejeitado porque mistura os arquivos de configuracao do dbt com os arquivos do projeto
  Python/Glue, tornando a raiz do repositório confusa
- **Repositório separado para o projeto dbt**: `bcb-warehouse-dbt` como repositório independente —
  rejeitado porque fragmenta o Projeto 2 em múltiplos repositórios sem benefício para o
  portfólio; a separacao de domínios dentro de um repositório é suficiente neste escopo

## Revisão
Elaborado por: Claude (Agente IA) — arquiteto-dados
Data/hora: 2026-07-18 09:00 BRT

## Aprovação
Aprovado por: Lucas de Araújo
Data/hora: 2026-07-20 13:05 BRT

## Errata — 2026-07-21

**`dim_serie.csv` renomeado para `dim_serie_seed.csv`.**

A estrutura do repositório listava `seeds/dim_serie.csv`. O arquivo foi renomeado para
`dim_serie_seed.csv` para evitar conflito de nomes no Athena: o dbt cria uma tabela
`dim_serie` (modelo `dim_serie.sql`) e o seed com o mesmo nome produziria uma colisão
— o Athena não permite VIEW e TABLE com o mesmo nome no mesmo database.

A convenção adotada: seeds que alimentam um modelo de mesmo nome recebem sufixo `_seed`
(`dim_serie_seed.csv`, `dim_data_seed.csv`). O modelo `dim_serie.sql` referencia o seed
via `ref('dim_serie_seed')`.

Estrutura corrigida:
```
seeds/
  dim_data_seed.csv          # calendário pré-gerado (2020-2030)
  feriados_nacionais.csv     # feriados nacionais brasileiros
  dim_serie_seed.csv         # metadados das 3 séries BCB (ver ADR-0009)
```

Corrigido por: Claude (Agente IA) — desenvolvedor-dados
Aprovação: Lucas de Araújo — 2026-07-21
