# ADR-0000: Convenções para registro de decisões arquiteturais

## Status
Aceito

## Contexto
Este repositório utiliza ADRs (Architecture Decision Records) para documentar decisões
arquiteturais relevantes. Para garantir consistência entre documentos e clareza sobre
autoria e aprovação, é necessário estabelecer um conjunto de convenções explícitas.

## Decisão

### Nomenclatura de arquivos

```
NNNN-titulo-da-decisao.md
```

- `NNNN`: número sequencial com quatro dígitos, iniciando em `0000`
- `titulo-da-decisao`: kebab-case, descritivo, sem artigos desnecessários
- Exemplos: `0001-formato-armazenamento-iceberg-vs-parquet-plano.md`, `0005-orquestracao-dag-estendida-vs-dag-separada.md`

### Status válidos

| Status | Significado |
|---|---|
| `Proposto` | Decisão documentada, aguardando aprovação |
| `Aceito` | Aprovado pelo responsável técnico do projeto |
| `Rejeitado` | Considerado e descartado — mantido para registro histórico |
| `Substituído` | Superado por outra ADR — o campo status deve conter `Substituído por ADR-NNNN` |

O status só muda para `Aceito` após aprovação explícita do responsável técnico.

### Estrutura mínima de seções

```markdown
# ADR-NNNN: Título

## Status
[Proposto | Aceito | Rejeitado | Substituído]

## Contexto
[Problema ou situação que motivou a decisão]

## Decisão
[O que foi decidido e por quê]

## Consequências
### Positivas
### Negativas / Trade-offs

## Alternativas consideradas
[Opções avaliadas e razão da rejeição]

## Relação com outras ADRs        ← opcional, incluir quando houver dependência
[Referências cruzadas]

## Revisão
Elaborado por: [autor]
Data/hora: YYYY-MM-DD HH:MM BRT
Revisado por: [revisor]              ← opcional, incluir quando houver revisão técnica
Data/hora: YYYY-MM-DD HH:MM BRT

## Aprovação                       ← preenchido somente após aprovação
Aprovado por: [responsável técnico]
Data/hora: YYYY-MM-DD HH:MM BRT
```

### Assinatura — padrão

**Seção `## Revisão`** — registra quem elaborou o documento:

```
## Revisão
Elaborado por: Claude (Agente IA) — [nome do agente]
Data/hora: YYYY-MM-DD HH:MM BRT
```

O campo `[nome do agente]` identifica o agente IA que elaborou o documento:
- Agente especializado: usar o identificador do agente (ex: `arquiteto-dados`)
- Sessão direta com o modelo: usar o identificador do modelo (ex: `claude-sonnet-4-6`)
- Autor humano: substituir toda a expressão pelo nome do autor

**Seção `## Aprovação`** — registra quem aprovou o documento:

```
## Aprovação
Aprovado por: Lucas de Araújo
Data/hora: YYYY-MM-DD HH:MM BRT
```

Regras:
- A seção `## Aprovação` só é adicionada após aprovação explícita — documentos `Proposto`
  não a contêm
- Quando o documento for elaborado por um humano, substituir `Claude (Agente IA)` pelo
  nome do autor
- O campo `Data/hora` usa o fuso horário BRT (UTC-3) em ambas as seções

### Idioma

- Português brasileiro em todo o conteúdo
- Nomes de ferramentas, serviços e identificadores técnicos mantidos no original
  (ex: `dbt`, `Iceberg`, `bcb_warehouse`, `fct_cotacoes_diarias`)

## Consequências

### Positivas
- Consistência entre todos os documentos do repositório
- Rastreabilidade clara de autoria (IA vs. humano) e aprovação
- Novos documentos têm um template explícito a seguir

### Negativas / Trade-offs
- ADRs criadas antes desta convenção precisam ser atualizadas retroativamente.
  Escopo da atualização retroativa para ADRs 0001–0008:
  - Substituir a seção `## Revisao` pelo formato padronizado com `Elaborado por:` e `Data/hora:`
  - Responsável: Lucas de Araújo (proprietário do projeto)
  - Nenhuma reaprovação é necessária — é uma correção de formatação, não de conteúdo

## Alternativas consideradas
- **Sem convenção formal**: cada ADR seguiria o estilo do autor — rejeitado por gerar
  inconsistência crescente à medida que o número de documentos aumenta

## Revisão
Elaborado por: Claude (Agente IA) — claude-sonnet-4-6
Data/hora: 2026-07-20 13:15 BRT
Revisado por: Claude (Agente IA) — arquiteto-dados
Data/hora: 2026-07-20 13:20 BRT

## Aprovação
Aprovado por: Lucas de Araújo
Data/hora: 2026-07-20 13:23 BRT
