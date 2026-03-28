"""
motor2/decision_engine.py — BLOCO 3
Engine de decisão: códigos, regras, OPME, score.

Funções:
  chooseCodes()         — seleciona códigos de 05_MAPEAMENTO_CODIGOS
  applyDecisionRules()  — executa regras de 09_REGRAS_DECISAO (if/then/else JSON)
  chooseOpme()          — constrói contexto OPME de 15_OPME_REGRAS + 16_OPME_CATALOGO
  calcScore()           — calcula score ponderado com fallback de métricas/pesos

Ordem de resolução de convênio em MAPEAMENTO_CODIGOS:
  1. Linhas com convenio_id == convenio_id do episódio
  2. Linhas com convenio_id == 'GLOBAL' (wildcard)

Score final:
  Componentes × pesos de 20_PESOS (ou default quando ausente).
  score_historico vem de 19_METRICAS; se vazio → 0.5 (neutro).
"""

import json
import logging
from dataclasses import dataclass, field
from typing import Any, Optional

from motor2.sheet_reader import SheetData, SheetReader

logger = logging.getLogger(__name__)

# ── Pesos default (quando convênio sem linha em 20_PESOS) ─────────────────────
DEFAULT_PESOS = {
    "peso_regulatorio": 0.25,
    "peso_convenio":    0.25,
    "peso_historico":   0.20,
    "peso_documental":  0.20,
    "peso_opme":        0.10,
}

# ── Estruturas de saída ────────────────────────────────────────────────────────

@dataclass
class CodigoDecidido:
    codigo: str
    descricao: str
    tipo_codigo: str          # principal | complementar
    codigo_sistema: str       # TUSS | CBHPM
    obrigatorio: bool
    risco_glosa_base: float
    ordem_sugestao: int
    condicao_uso: Optional[dict]
    fonte: str                # convenio_id ou GLOBAL


@dataclass
class OpmeDecisao:
    items_requeridos: list[dict]    # da 15_OPME_REGRAS
    catalogo_sugerido: list[dict]   # da 16_OPME_CATALOGO (por categoria)
    alertas_opme: list[str]
    warnings: list[str] = field(default_factory=list)


@dataclass
class RegraAplicada:
    regra_id: str
    nome_regra: str
    tipo_regra: str           # alerta | bloqueio | info
    prioridade_execucao: int
    resultado: str            # then | else | skipped
    acoes: dict               # conteúdo de then_json ou else_json


@dataclass
class ScoreResumo:
    score_final: float
    score_regulatorio: float
    score_convenio: float
    score_historico: float
    score_documental: float
    score_opme: float
    pesos_usados: dict
    fonte_historico: str      # metricas | neutro_fallback


@dataclass
class DecisionEngineResult:
    profile_id: str
    convenio_id: str
    codigos: list[CodigoDecidido]
    regras_aplicadas: list[RegraAplicada]
    alertas: list[str]
    bloqueios: list[str]
    opme_decisao: Optional[OpmeDecisao]
    score_resumo: ScoreResumo
    warnings: list[str] = field(default_factory=list)

    @property
    def codigo_principal(self) -> Optional[CodigoDecidido]:
        principals = [c for c in self.codigos if c.tipo_codigo == "principal"]
        if not principals:
            return None
        return sorted(principals, key=lambda c: c.ordem_sugestao)[0]

    @property
    def codigos_complementares(self) -> list[CodigoDecidido]:
        return [c for c in self.codigos if c.tipo_codigo == "complementar"]


# ── chooseCodes ───────────────────────────────────────────────────────────────

def chooseCodes(
    profile_id: str,
    convenio_id: str,
    carater: str,
    niveis: int,
    mapeamento_sheet: SheetData,
    bloqueio_sheet: SheetData,
) -> tuple[list[CodigoDecidido], list[str]]:
    """
    Seleciona códigos de 05_MAPEAMENTO_CODIGOS.

    Filtros aplicados:
      - profile_id exato
      - convenio_id exato OU GLOBAL
      - ativo = True
      - niveis_min <= niveis <= niveis_max
      - aceita_qualquer_carater OU contexto_carater == carater

    Bloqueios de coexistência (06_REL_COD_BLOQUEIO) são verificados
    após seleção.

    Retorna (lista_de_codigos, lista_de_warnings).
    """
    warnings: list[str] = []

    # Filtro base: profile + ativo
    candidates = [
        r for r in mapeamento_sheet.rows
        if r.get("profile_id") == profile_id and r.get("ativo") is not False
    ]

    if not candidates:
        warnings.append(
            f"codigos_ausentes: nenhum código em 05_MAPEAMENTO_CODIGOS "
            f"para profile_id '{profile_id}'"
        )
        return [], warnings

    # Filtro por convênio: preferir match exato, fallback GLOBAL
    convenio_rows = [r for r in candidates if r.get("convenio_id") == convenio_id]
    if not convenio_rows:
        convenio_rows = [r for r in candidates if r.get("convenio_id") == "GLOBAL"]
        if convenio_rows:
            warnings.append(
                f"codigo_convenio_fallback: usando códigos GLOBAL para convênio '{convenio_id}'"
            )
        else:
            warnings.append(
                f"codigos_sem_convenio: nenhum código para convenio '{convenio_id}' "
                f"nem GLOBAL para profile '{profile_id}'"
            )
            return [], warnings

    # Filtro por niveis
    nivel_rows = [
        r for r in convenio_rows
        if (r.get("niveis_min") is None or int(r["niveis_min"]) <= niveis)
        and (r.get("niveis_max") is None or niveis <= int(r["niveis_max"]))
    ]
    if not nivel_rows:
        nivel_rows = convenio_rows  # fallback: usar sem filtro de nível
        warnings.append(
            f"codigo_nivel_fallback: nenhum código para niveis={niveis}, "
            f"usando todos os códigos do convênio sem filtro de nível"
        )

    # Filtro por carater
    carater_rows = [
        r for r in nivel_rows
        if r.get("aceita_qualquer_carater")
        or r.get("contexto_carater") == carater
        or r.get("contexto_carater") is None
    ]
    if not carater_rows:
        carater_rows = nivel_rows  # fallback sem filtro de carater
        warnings.append(
            f"codigo_carater_fallback: nenhum código para carater='{carater}', "
            f"usando sem filtro de carater"
        )

    # Verificar bloqueios de coexistência
    selected_codes = {r.get("codigo") for r in carater_rows}
    bloqueios_profile = [
        b for b in bloqueio_sheet.rows
        if b.get("profile_id") == profile_id and b.get("ativo") is not False
        and (b.get("convenio_id") in (convenio_id, "GLOBAL", None))
    ]
    blocked_codes: set[str] = set()
    for bloq in bloqueios_profile:
        if bloq.get("codigo_origem") in selected_codes:
            blocked = bloq.get("codigo_bloqueado")
            if blocked in selected_codes:
                blocked_codes.add(blocked)
                warnings.append(
                    f"codigo_bloqueado: '{blocked}' bloqueado por coexistência com "
                    f"'{bloq['codigo_origem']}' — {bloq.get('motivo_bloqueio', '')}"
                )

    # Montar lista final
    result: list[CodigoDecidido] = []
    for r in carater_rows:
        if r.get("codigo") in blocked_codes:
            continue
        cond = r.get("condicao_uso_json")  # já parseado pelo reader
        result.append(CodigoDecidido(
            codigo=str(r.get("codigo", "")),
            descricao=str(r.get("descricao_codigo", "")),
            tipo_codigo=str(r.get("tipo_codigo", "complementar")),
            codigo_sistema=str(r.get("codigo_sistema", "TUSS")),
            obrigatorio=bool(r.get("obrigatorio")),
            risco_glosa_base=float(r.get("risco_glosa_base") or 0.0),
            ordem_sugestao=int(r.get("ordem_sugestao") or 99),
            condicao_uso=cond if isinstance(cond, dict) else None,
            fonte=str(r.get("convenio_id", "GLOBAL")),
        ))

    # Ordenar: principais primeiro, depois por ordem_sugestao
    result.sort(key=lambda c: (0 if c.tipo_codigo == "principal" else 1, c.ordem_sugestao))

    return result, warnings


# ── applyDecisionRules ────────────────────────────────────────────────────────

def _eval_condition(condition: Any, context: dict) -> bool:
    """
    Avalia uma condição JSON simples contra o contexto do episódio.

    Formato suportado:
      {"chave": valor}                  → context["chave"] == valor
      {"chave": {"gt": n}}              → context["chave"] > n
      {"chave": {"gte": n}}             → context["chave"] >= n
      {"chave": {"lt": n}}              → context["chave"] < n
      {"chave": {"lte": n}}             → context["chave"] <= n
      {"chave1": v1, "chave2": v2}      → AND de todas as condições

    Acesso a campos aninhados via ponto: "clinical_context.tto_conservador_semanas"
    Retorna True se condição se aplica, False caso contrário.
    Retorna True para condições None/vazias (sem restrição).
    """
    if not condition or not isinstance(condition, dict):
        return True

    def _get_val(ctx: dict, key: str) -> Any:
        """Acesso aninhado com ponto: 'a.b.c'
        Tenta chave literal primeiro (suporta chaves com ponto como
        'clinical_context.tto_conservador_semanas' armazenadas flat).
        """
        # 1. Chave literal (ex: full_context["clinical_context.tto_conservador_semanas"])
        if key in ctx:
            return ctx[key]
        # 2. Navegação aninhada real (ex: ctx["clinical_context"]["tto_conservador_semanas"])
        parts = key.split(".")
        cur = ctx
        for p in parts:
            if isinstance(cur, dict):
                cur = cur.get(p)
            else:
                return None
        return cur

    for key, expected in condition.items():
        actual = _get_val(context, key)

        if isinstance(expected, dict):
            # Operadores de comparação
            for op, val in expected.items():
                try:
                    act_num = float(actual) if actual is not None else None
                    val_num = float(val)
                    if op == "gt" and not (act_num is not None and act_num > val_num):
                        return False
                    if op == "gte" and not (act_num is not None and act_num >= val_num):
                        return False
                    if op == "lt" and not (act_num is not None and act_num < val_num):
                        return False
                    if op == "lte" and not (act_num is not None and act_num <= val_num):
                        return False
                except (TypeError, ValueError):
                    return False
        else:
            # Match direto
            if actual != expected:
                return False

    return True


def applyDecisionRules(
    context: dict,
    regras_sheet: SheetData,
    profile_id: str,
    convenio_id: str,
) -> tuple[list[RegraAplicada], list[str], list[str]]:
    """
    Aplica regras de 09_REGRAS_DECISAO ao contexto do caso.

    Filtra regras ativas para o profile (ou profile_id=None = global)
    e o convênio (ou GLOBAL).
    Executa em ordem crescente de prioridade_execucao.

    Retorna (regras_aplicadas, alertas, bloqueios).
    """
    alertas: list[str] = []
    bloqueios: list[str] = []
    regras_aplicadas: list[RegraAplicada] = []

    # Filtrar regras relevantes
    regras = [
        r for r in regras_sheet.rows
        if r.get("ativo") is not False
        and (r.get("profile_id") is None or r.get("profile_id") == profile_id)
        and (r.get("convenio_id") in (convenio_id, "GLOBAL", None))
    ]

    # Ordenar por prioridade (ASC — menor número = maior prioridade)
    regras.sort(key=lambda r: int(r.get("prioridade_execucao") or 99))

    for regra in regras:
        if_cond = regra.get("if_json")  # já parseado pelo reader
        then_act = regra.get("then_json")
        else_act = regra.get("else_json")

        condition_met = _eval_condition(if_cond, context)

        if condition_met:
            acoes = then_act if isinstance(then_act, dict) else {}
            resultado = "then"
        elif else_act:
            acoes = else_act if isinstance(else_act, dict) else {}
            resultado = "else"
        else:
            resultado = "skipped"
            acoes = {}

        tipo = str(regra.get("tipo_regra", "info"))

        if resultado != "skipped" and acoes:
            # Processar ações
            for alerta_msg in acoes.get("add_alerts", []):
                alertas.append(f"[{regra.get('regra_id')}] {alerta_msg}")
            for bloq_msg in acoes.get("add_blocks", []):
                bloqueios.append(f"[{regra.get('regra_id')}] {bloq_msg}")

            regras_aplicadas.append(RegraAplicada(
                regra_id=str(regra.get("regra_id", "")),
                nome_regra=str(regra.get("nome_regra", "")),
                tipo_regra=tipo,
                prioridade_execucao=int(regra.get("prioridade_execucao") or 99),
                resultado=resultado,
                acoes=acoes,
            ))
        else:
            # Registrar regras skipped/sem ação para rastreabilidade
            regras_aplicadas.append(RegraAplicada(
                regra_id=str(regra.get("regra_id", "")),
                nome_regra=str(regra.get("nome_regra", "")),
                tipo_regra=tipo,
                prioridade_execucao=int(regra.get("prioridade_execucao") or 99),
                resultado=resultado,
                acoes={},
            ))

    return regras_aplicadas, alertas, bloqueios


# ── chooseOpme ────────────────────────────────────────────────────────────────

def chooseOpme(
    profile_id: str,
    convenio_id: str,
    opme_context: Optional[dict],
    opme_regras_sheet: SheetData,
    opme_catalogo_sheet: SheetData,
) -> tuple[Optional[OpmeDecisao], list[str]]:
    """
    Constrói a decisão de OPME para o caso.

    Se profile não permite OPME (verificado externamente) ou opme_context é None,
    retorna (None, []).

    Valida:
      - marcas_minimas (RN 424/2017): mínimo 3 fabricantes
      - anvisa_obrigatoria: registro ANVISA presente
      - quantidade dentro de min/max

    Sugere itens de catálogo por categoria de cada regra OPME.
    """
    warnings: list[str] = []
    alertas_opme: list[str] = []

    if opme_context is None:
        return None, []

    # Regras OPME do profile (com fallback GLOBAL por convenio)
    regras = [
        r for r in opme_regras_sheet.rows
        if r.get("profile_id") == profile_id
        and r.get("ativo") is not False
        and (r.get("convenio_id") in (convenio_id, "GLOBAL", None))
    ]

    if not regras:
        regras = [
            r for r in opme_regras_sheet.rows
            if r.get("profile_id") == profile_id and r.get("ativo") is not False
        ]
        if regras:
            warnings.append(f"opme_regras_global: usando regras OPME globais para '{profile_id}'")
        else:
            warnings.append(f"opme_regras_ausentes: sem regras OPME para '{profile_id}'")

    items_requeridos: list[dict] = []
    for regra in regras:
        item = {
            "item_nome": regra.get("item_nome"),
            "tipo_item": regra.get("tipo_item"),
            "codigo_referencia": regra.get("codigo_referencia"),
            "anvisa_obrigatoria": bool(regra.get("anvisa_obrigatoria")),
            "marcas_minimas": int(regra.get("marcas_minimas") or 3),
            "quantidade_min": int(regra.get("quantidade_min") or 1),
            "quantidade_max": int(regra.get("quantidade_max") or 1),
            "justificativa_obrigatoria": bool(regra.get("justificativa_obrigatoria")),
        }
        items_requeridos.append(item)

        # Validar marcas no contexto
        num_marcas = opme_context.get("numero_marcas") or 0
        min_marcas = item["marcas_minimas"]
        if num_marcas < min_marcas:
            alertas_opme.append(
                f"opme_marcas_insuficientes: '{item['item_nome']}' exige ≥{min_marcas} marcas "
                f"(RN 424/2017), recebido {num_marcas}"
            )

        # Validar ANVISA — uma vez por item, independente de quantas regras exigem
        if item["anvisa_obrigatoria"]:
            itens_contexto = opme_context.get("itens", [])
            for item_ctx in itens_contexto:
                if not item_ctx.get("registro_anvisa"):
                    alerta_msg = (
                        f"opme_anvisa_ausente: item '{item_ctx.get('item')}' "
                        f"sem registro ANVISA"
                    )
                    if alerta_msg not in alertas_opme:
                        alertas_opme.append(alerta_msg)

    # Buscar catálogo por categoria de cada regra
    catalogo_sugerido: list[dict] = []
    categorias_buscadas: set[str] = set()

    for regra in regras:
        item_nome = str(regra.get("item_nome", "")).lower()
        for cat_row in opme_catalogo_sheet.rows:
            if cat_row.get("ativo") is False:
                continue
            categoria = str(cat_row.get("categoria", "")).lower()
            cat_id = cat_row.get("opme_item_id")
            if cat_id in categorias_buscadas:
                continue
            if categoria in item_nome or item_nome in categoria:
                catalogo_sugerido.append({
                    "opme_item_id": cat_row.get("opme_item_id"),
                    "fabricante": cat_row.get("fabricante"),
                    "nome_comercial": cat_row.get("nome_comercial"),
                    "categoria": cat_row.get("categoria"),
                    "registro_anvisa": cat_row.get("registro_anvisa"),
                })
                categorias_buscadas.add(cat_id)

    return OpmeDecisao(
        items_requeridos=items_requeridos,
        catalogo_sugerido=catalogo_sugerido,
        alertas_opme=alertas_opme,
        warnings=warnings,
    ), warnings


# ── calcScore ─────────────────────────────────────────────────────────────────

def calcScore(
    profile_id: str,
    convenio_id: str,
    hospital_id: Optional[str],
    cid_resolution: Any,      # CIDResolution
    cbo_resolution: Any,      # CBOResolution
    codigos: list[CodigoDecidido],
    opme_decisao: Optional[OpmeDecisao],
    alertas: list[str],
    bloqueios: list[str],
    metricas_sheet: SheetData,
    pesos_sheet: SheetData,
) -> ScoreResumo:
    """
    Calcula o score ponderado de confiança da decisão.

    Componentes:
      score_regulatorio  — CID válido, CBO válido, código principal presente
      score_convenio     — sem alertas de bloqueio de convênio
      score_historico    — taxa_autorizacao de METRICAS (ou 0.5 neutro)
      score_documental   — presença de código principal obrigatório
      score_opme         — OPME com marcas suficientes e ANVISA presente

    score_final = sum(componente_i × peso_i)
    """
    # Pesos: match exato → GLOBAL → linha com convenio_id=None → hardcoded default
    pesos_row = pesos_sheet.find_one(convenio_id=convenio_id)
    if not pesos_row:
        pesos_row = pesos_sheet.find_one(convenio_id="GLOBAL")
    if not pesos_row:
        # Linha global com convenio_id=None (PESO_0001 na planilha)
        pesos_row = next(
            (r for r in pesos_sheet.rows if r.get("convenio_id") is None and r.get("ativo") is not False),
            None,
        )
    if pesos_row:
        pesos = {k: float(pesos_row.get(k) or DEFAULT_PESOS[k]) for k in DEFAULT_PESOS}
        fonte_pesos = f"20_PESOS[{pesos_row.get('convenio_id', 'GLOBAL_DEFAULT')}]"
    else:
        pesos = dict(DEFAULT_PESOS)
        fonte_pesos = "hardcoded_fallback"

    # score_historico: de METRICAS ou neutro
    fonte_historico = "neutro_fallback"
    score_historico = 0.5

    metricas_rows = metricas_sheet.filter(profile_id=profile_id, convenio_id=convenio_id)
    if not metricas_rows and convenio_id != "GLOBAL":
        metricas_rows = metricas_sheet.filter(profile_id=profile_id)
    if metricas_rows:
        m = metricas_rows[0]
        taxa_aut = m.get("taxa_autorizacao")
        taxa_glosa = m.get("taxa_glosa")
        if taxa_aut is not None:
            score_historico = float(taxa_aut)
            fonte_historico = f"19_METRICAS[{profile_id}]"

    # score_regulatorio: CID + CBO válidos + código principal presente
    cid_ok = getattr(cid_resolution, "is_valid", True)
    cbo_ok = getattr(cbo_resolution, "is_valid", True)
    has_principal = any(c.tipo_codigo == "principal" for c in codigos)
    score_regulatorio = (
        (1.0 if cid_ok else 0.3)
        + (0.5 if cbo_ok else 0.2)
        + (0.5 if has_principal else 0.0)
    ) / 2.0
    score_regulatorio = min(1.0, score_regulatorio)

    # score_convenio: penaliza por alertas e bloqueios de convênio
    n_bloq = len(bloqueios)
    n_alert = len(alertas)
    score_convenio = max(0.0, 1.0 - (n_bloq * 0.3) - (n_alert * 0.05))

    # score_documental: presença de código obrigatório
    mandatory_codes = [c for c in codigos if c.obrigatorio]
    score_documental = 1.0 if mandatory_codes else 0.5

    # score_opme: sem alertas de OPME = 1.0
    if opme_decisao is None:
        score_opme = 1.0  # sem OPME = sem problema de OPME
    else:
        n_opme_alertas = len(opme_decisao.alertas_opme)
        score_opme = max(0.0, 1.0 - (n_opme_alertas * 0.25))

    # Score final ponderado
    score_final = (
        score_regulatorio * pesos["peso_regulatorio"]
        + score_convenio   * pesos["peso_convenio"]
        + score_historico  * pesos["peso_historico"]
        + score_documental * pesos["peso_documental"]
        + score_opme       * pesos["peso_opme"]
    )
    score_final = round(min(1.0, max(0.0, score_final)), 4)

    return ScoreResumo(
        score_final=score_final,
        score_regulatorio=round(score_regulatorio, 4),
        score_convenio=round(score_convenio, 4),
        score_historico=round(score_historico, 4),
        score_documental=round(score_documental, 4),
        score_opme=round(score_opme, 4),
        pesos_usados={**pesos, "fonte": fonte_pesos},
        fonte_historico=fonte_historico,
    )


# ── Entrada principal do engine ───────────────────────────────────────────────

def run_decision_engine(
    profile_id: str,
    convenio_id: str,
    hospital_id: Optional[str],
    carater: str,
    niveis: int,
    cid_resolution: Any,    # CIDResolution (de resolver.py)
    cbo_resolution: Any,    # CBOResolution (de resolver.py)
    opme_context: Optional[dict],
    clinical_context: Optional[dict],
    reader: SheetReader,
) -> DecisionEngineResult:
    """
    Orquestra todo o BLOCO 3: chooseCodes → applyRules → chooseOpme → calcScore.
    """
    warnings: list[str] = []

    # Contexto completo para avaliação de regras
    full_context = {
        "profile_id":    profile_id,
        "convenio_id":   convenio_id,
        "hospital_id":   hospital_id,
        "carater":       carater,
        "niveis":        niveis,
        "cid_principal": getattr(cid_resolution, "cid", ""),
        "cbo_executor":  getattr(cbo_resolution, "cbo", None),
        **(clinical_context or {}),
    }

    # Calcular indicacao_len para regra de completude
    indicacao = (clinical_context or {}).get("indicacao_clinica", "")
    full_context["clinical_context.indicacao_len"] = len(str(indicacao))
    full_context["clinical_context.tto_conservador_semanas"] = (
        (clinical_context or {}).get("tto_conservador_semanas")
    )

    # 1. Códigos
    codigos, cod_warnings = chooseCodes(
        profile_id=profile_id,
        convenio_id=convenio_id,
        carater=carater,
        niveis=niveis,
        mapeamento_sheet=reader.get("05_MAPEAMENTO_CODIGOS"),
        bloqueio_sheet=reader.get("06_REL_COD_BLOQUEIO"),
    )
    warnings.extend(cod_warnings)

    # 2. Regras
    regras_aplicadas, alertas, bloqueios = applyDecisionRules(
        context=full_context,
        regras_sheet=reader.get("09_REGRAS_DECISAO"),
        profile_id=profile_id,
        convenio_id=convenio_id,
    )
    # CID em alerta gera alerta direto
    warnings.extend(getattr(cid_resolution, "warnings", []))
    warnings.extend(getattr(cbo_resolution, "warnings", []))
    if getattr(cid_resolution, "requires_justification", False):
        alertas.append(
            f"cid_justificativa: CID '{cid_resolution.cid}' requer justificativa reforçada"
        )

    # 3. OPME
    opme_decisao, opme_warn = chooseOpme(
        profile_id=profile_id,
        convenio_id=convenio_id,
        opme_context=opme_context,
        opme_regras_sheet=reader.get("15_OPME_REGRAS"),
        opme_catalogo_sheet=reader.get("16_OPME_CATALOGO"),
    )
    warnings.extend(opme_warn)
    if opme_decisao:
        alertas.extend(opme_decisao.alertas_opme)

    # 4. Score
    score_resumo = calcScore(
        profile_id=profile_id,
        convenio_id=convenio_id,
        hospital_id=hospital_id,
        cid_resolution=cid_resolution,
        cbo_resolution=cbo_resolution,
        codigos=codigos,
        opme_decisao=opme_decisao,
        alertas=alertas,
        bloqueios=bloqueios,
        metricas_sheet=reader.get("19_METRICAS"),
        pesos_sheet=reader.get("20_PESOS"),
    )

    return DecisionEngineResult(
        profile_id=profile_id,
        convenio_id=convenio_id,
        codigos=codigos,
        regras_aplicadas=regras_aplicadas,
        alertas=alertas,
        bloqueios=bloqueios,
        opme_decisao=opme_decisao,
        score_resumo=score_resumo,
        warnings=warnings,
    )
