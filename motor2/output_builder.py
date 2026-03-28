"""
motor2/output_builder.py — BLOCO 4
Construção do artefato de saída, determinação de status e persistência.

Funções:
  buildDocumentPayload()  — monta payload documental de 12_MODELOS_DOCUM + 13 + 14
  setDecisionStatus()     — GO | GO_COM_RESSALVAS | NO_GO
  writeDecisionRun()      — persiste em SQLite (schema de 21_DECISION_RUNS)
  run_motor2()            — orquestrador completo: resolver → engine → output

Output estruturado (DecisionArtefact):
  decision_id           UUID da decisão
  episodio_id           referência ao episódio do Motor 1
  profile_id            profile resolvido
  codigo_principal      código principal escolhido
  codigos_complementares lista de complementares
  opme_escolhida        resumo de OPME
  regras_aplicadas      lista de regras executadas
  score_resumo          score ponderado e componentes
  payload_documentos    modelos + blocos documentais
  status_decisao        GO | GO_COM_RESSALVAS | NO_GO
  alertas               lista de alertas
  bloqueios             lista de bloqueios
  warnings_estruturais  gaps da planilha e warnings do motor
  created_at            timestamp ISO 8601
"""

import json
import uuid
import sqlite3
import logging
import os
from dataclasses import dataclass, asdict, field
from datetime import datetime, timezone
from typing import Any, Optional

from motor2.sheet_reader import SheetData, SheetReader, get_reader
from motor2.resolver import (
    resolveProfileFromAlias,
    resolveConvenio,
    resolveHospital,
    resolveCIDRules,
    resolveCBORules,
)
from motor2.decision_engine import (
    run_decision_engine,
    DecisionEngineResult,
    CodigoDecidido,
    OpmeDecisao,
    ScoreResumo,
)

logger = logging.getLogger(__name__)

# SQLite — mesmo diretório que Motor 1 usa (/tmp/neuroauth.db)
DECISAO_DB_PATH = os.environ.get("DECISAO_DB_PATH", "/tmp/neuroauth_motor2.db")

# ── Estrutura de saída ─────────────────────────────────────────────────────────

@dataclass
class DocumentoSugerido:
    modelo_id: str
    tipo_documento: str
    nome_modelo: str
    template_texto: str
    blocos_ativos: list[dict]   # blocos cujo gatilho foi atendido


@dataclass
class DecisionArtefact:
    decision_id: str
    episodio_id: str
    profile_id: Optional[str]
    alias_matched: Optional[str]
    codigo_principal: Optional[dict]
    codigos_complementares: list[dict]
    opme_escolhida: Optional[dict]
    regras_aplicadas: list[dict]
    score_resumo: dict
    payload_documentos: list[dict]
    status_decisao: str              # GO | GO_COM_RESSALVAS | NO_GO
    alertas: list[str]
    bloqueios: list[str]
    warnings_estruturais: list[str]
    created_at: str

    def to_dict(self) -> dict:
        return asdict(self)


# ── buildDocumentPayload ───────────────────────────────────────────────────────

def buildDocumentPayload(
    profile_id: str,
    convenio_id: str,
    clinical_context: Optional[dict],
    modelos_sheet: SheetData,
    blocos_sheet: SheetData,
    rel_sheet: SheetData,
) -> list[DocumentoSugerido]:
    """
    Monta payload documental a partir de 12_MODELOS_DOCUM + 13_BLOCOS_DOCUMENTAIS + 14_MOD_BLOCO_REL.

    Para cada modelo ativo do profile/convenio:
      1. Busca relações modelo↔bloco em 14_MOD_BLOCO_REL
      2. Para cada bloco relacionado, avalia gatilho_json contra clinical_context
      3. Ativa blocos cujo gatilho é atendido (ou que são obrigatórios)
    """
    features = clinical_context or {}

    # Modelos: match exato ou GLOBAL para convenio
    modelos = [
        m for m in modelos_sheet.rows
        if m.get("profile_id") == profile_id
        and m.get("ativo") is not False
        and (m.get("convenio_id") in (convenio_id, "GLOBAL", None))
    ]

    if not modelos:
        logger.warning("buildDocumentPayload: sem modelos para '%s'/'%s'", profile_id, convenio_id)

    documentos: list[DocumentoSugerido] = []

    for modelo in modelos:
        modelo_id = modelo.get("modelo_id")

        # Buscar relações deste modelo com blocos
        relacoes = [r for r in rel_sheet.rows if r.get("modelo_id") == modelo_id]

        blocos_ativos: list[dict] = []

        for rel in relacoes:
            bloco_id = rel.get("bloco_id")
            obrigatorio = bool(rel.get("obrigatorio"))

            bloco_row = blocos_sheet.find_one(bloco_id=bloco_id, ativo=True)
            if not bloco_row:
                continue

            gatilho = bloco_row.get("gatilho_json")  # já parseado

            # Avaliar gatilho
            gatilho_atendido = _eval_gatilho(gatilho, features)

            if obrigatorio or gatilho_atendido:
                blocos_ativos.append({
                    "bloco_id": bloco_id,
                    "nome_bloco": bloco_row.get("nome_bloco"),
                    "texto_bloco": bloco_row.get("texto_bloco"),
                    "ordem_insercao": rel.get("ordem_override") or bloco_row.get("ordem_insercao"),
                    "obrigatorio": obrigatorio,
                    "gatilho_atendido": gatilho_atendido,
                })

        # Ordenar blocos por ordem_insercao
        blocos_ativos.sort(key=lambda b: float(b.get("ordem_insercao") or 99))

        documentos.append(DocumentoSugerido(
            modelo_id=str(modelo_id or ""),
            tipo_documento=str(modelo.get("tipo_documento", "")),
            nome_modelo=str(modelo.get("nome_modelo", "")),
            template_texto=str(modelo.get("template_texto", "")),
            blocos_ativos=blocos_ativos,
        ))

    return documentos


def _eval_gatilho(gatilho: Any, features: dict) -> bool:
    """
    Avalia um gatilho_json de bloco documental.

    Formato: {"features.campo": valor} ou {"campo": valor}
    Retorna True se todos os pares forem satisfeitos.
    """
    if not gatilho or not isinstance(gatilho, dict):
        return True

    for key, expected in gatilho.items():
        # Normaliza chave: "features.falha_conservador" → "falha_conservador"
        actual_key = key.replace("features.", "")
        actual = features.get(actual_key)
        if actual != expected:
            return False

    return True


# ── setDecisionStatus ─────────────────────────────────────────────────────────

def setDecisionStatus(
    profile_resolution_ok: bool,
    alertas: list[str],
    bloqueios: list[str],
    codigos: list[CodigoDecidido],
    warnings: list[str],
) -> str:
    """
    Determina o status final da decisão.

    GO             → sem bloqueios, profile master presente, código principal encontrado
    GO_COM_RESSALVAS → sem bloqueios, mas com alertas OU profile ausente do master OU sem código principal
    NO_GO          → há bloqueios
    """
    if bloqueios:
        return "NO_GO"

    has_principal = any(c.tipo_codigo == "principal" for c in codigos)
    master_missing = any("profile_master_missing" in w for w in warnings)

    if alertas or not has_principal or not profile_resolution_ok or master_missing:
        return "GO_COM_RESSALVAS"

    return "GO"


# ── writeDecisionRun ──────────────────────────────────────────────────────────

def _init_decisao_db():
    """Cria tabela decision_runs se não existir."""
    with sqlite3.connect(DECISAO_DB_PATH) as conn:
        conn.execute("""
            CREATE TABLE IF NOT EXISTS decision_runs (
                decision_run_id    TEXT PRIMARY KEY,
                episodio_id        TEXT,
                profile_id         TEXT,
                input_context_json TEXT,
                opcoes_geradas_json TEXT,
                opcao_escolhida_json TEXT,
                score_final        REAL,
                alertas_json       TEXT,
                bloqueios_json     TEXT,
                status_decisao     TEXT,
                warnings_json      TEXT,
                motor_version      TEXT DEFAULT 'motor2_v1',
                created_at         TEXT
            )
        """)
        conn.commit()


def writeDecisionRun(artefact: DecisionArtefact) -> str:
    """
    Persiste o DecisionArtefact na tabela decision_runs (SQLite).
    Retorna o decision_run_id.
    Idempotente por decision_id.
    """
    _init_decisao_db()

    with sqlite3.connect(DECISAO_DB_PATH) as conn:
        conn.execute("""
            INSERT OR IGNORE INTO decision_runs (
                decision_run_id, episodio_id, profile_id,
                input_context_json, opcoes_geradas_json, opcao_escolhida_json,
                score_final, alertas_json, bloqueios_json,
                status_decisao, warnings_json, motor_version, created_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            artefact.decision_id,
            artefact.episodio_id,
            artefact.profile_id,
            json.dumps({}, ensure_ascii=False),  # input armazenado no episodio
            json.dumps(artefact.codigos_complementares, ensure_ascii=False),
            json.dumps(artefact.codigo_principal, ensure_ascii=False),
            artefact.score_resumo.get("score_final", 0.0),
            json.dumps(artefact.alertas, ensure_ascii=False),
            json.dumps(artefact.bloqueios, ensure_ascii=False),
            artefact.status_decisao,
            json.dumps(artefact.warnings_estruturais, ensure_ascii=False),
            "motor2_v1",
            artefact.created_at,
        ))
        conn.commit()

    return artefact.decision_id


def listDecisionRuns(episodio_id: Optional[str] = None, limite: int = 50) -> list[dict]:
    """Lista decision runs do SQLite."""
    _init_decisao_db()
    with sqlite3.connect(DECISAO_DB_PATH) as conn:
        conn.row_factory = sqlite3.Row
        if episodio_id:
            rows = conn.execute(
                "SELECT * FROM decision_runs WHERE episodio_id=? ORDER BY created_at DESC LIMIT ?",
                (episodio_id, limite),
            ).fetchall()
        else:
            rows = conn.execute(
                "SELECT * FROM decision_runs ORDER BY created_at DESC LIMIT ?",
                (limite,),
            ).fetchall()
    return [dict(r) for r in rows]


def getDecisionRun(decision_id: str) -> Optional[dict]:
    """Retorna um decision run específico."""
    _init_decisao_db()
    with sqlite3.connect(DECISAO_DB_PATH) as conn:
        conn.row_factory = sqlite3.Row
        row = conn.execute(
            "SELECT * FROM decision_runs WHERE decision_run_id=?",
            (decision_id,),
        ).fetchone()
    return dict(row) if row else None


# ── Orquestrador completo ─────────────────────────────────────────────────────

def run_motor2(
    episodio_id: str,
    profile_texto: str,
    convenio_id: str,
    hospital_id: Optional[str],
    carater: str,
    niveis: int,
    cid_principal: str,
    cid_secundarios: list[str],
    cbo_executor: Optional[str],
    opme_context: Optional[dict],
    clinical_context: Optional[dict],
    xlsx_path: Optional[str] = None,
) -> DecisionArtefact:
    """
    Ponto de entrada único do Motor 2.

    Executa na ordem:
      1. SheetReader.load()
      2. resolveProfileFromAlias()
      3. resolveConvenio / resolveHospital / resolveCIDRules / resolveCBORules
      4. run_decision_engine() — chooseCodes + applyRules + chooseOpme + calcScore
      5. buildDocumentPayload()
      6. setDecisionStatus()
      7. writeDecisionRun()

    Nunca levanta exceção por ausência de dados históricos ou de profile no master.
    Warnings estruturais são acumulados no artefato de saída.
    """
    decision_id = f"DR_{uuid.uuid4()}"
    created_at = datetime.now(timezone.utc).isoformat()
    all_warnings: list[str] = []

    # Carregar planilha
    reader = get_reader(xlsx_path)
    all_warnings.extend(reader.structural_warnings)

    # ── BLOCO 2: Resolução ────────────────────────────────────────────────────
    profile_res = resolveProfileFromAlias(
        texto=profile_texto,
        alias_sheet=reader.get("02_PROC_ALIAS"),
        proc_mestre_sheet=reader.get("01_PROC_MESTRE"),
    )
    all_warnings.extend(profile_res.warnings)

    profile_id = profile_res.profile_id or profile_texto  # fallback: usar texto como ID

    cid_res = resolveCIDRules(
        profile_id=profile_id,
        cid_principal=cid_principal,
        cids_sheet=reader.get("07_CIDS_PERMITIDOS"),
    )
    all_warnings.extend(cid_res.warnings)

    cbo_res = resolveCBORules(
        profile_id=profile_id,
        cbo_executor=cbo_executor,
        convenio_id=convenio_id,
        cbos_sheet=reader.get("08_CBOS_PERMITIDOS"),
    )
    all_warnings.extend(cbo_res.warnings)

    # ── BLOCO 3: Engine de decisão ────────────────────────────────────────────
    engine_result: DecisionEngineResult = run_decision_engine(
        profile_id=profile_id,
        convenio_id=convenio_id,
        hospital_id=hospital_id,
        carater=carater,
        niveis=niveis,
        cid_resolution=cid_res,
        cbo_resolution=cbo_res,
        opme_context=opme_context,
        clinical_context=clinical_context,
        reader=reader,
    )
    all_warnings.extend(engine_result.warnings)

    # ── BLOCO 4a: Documentos ──────────────────────────────────────────────────
    documentos = buildDocumentPayload(
        profile_id=profile_id,
        convenio_id=convenio_id,
        clinical_context=clinical_context,
        modelos_sheet=reader.get("12_MODELOS_DOCUM"),
        blocos_sheet=reader.get("13_BLOCOS_DOCUMENTAIS"),
        rel_sheet=reader.get("14_MOD_BLOCO_REL"),
    )

    # ── BLOCO 4b: Status ──────────────────────────────────────────────────────
    status = setDecisionStatus(
        profile_resolution_ok=profile_res.resolved,
        alertas=engine_result.alertas,
        bloqueios=engine_result.bloqueios,
        codigos=engine_result.codigos,
        warnings=all_warnings,
    )

    # ── Serialização do artefato ──────────────────────────────────────────────
    def _serialize_codigo(c: CodigoDecidido) -> dict:
        return {
            "codigo": c.codigo,
            "descricao": c.descricao,
            "tipo_codigo": c.tipo_codigo,
            "codigo_sistema": c.codigo_sistema,
            "obrigatorio": c.obrigatorio,
            "risco_glosa_base": c.risco_glosa_base,
            "ordem_sugestao": c.ordem_sugestao,
            "condicao_uso": c.condicao_uso,
            "fonte": c.fonte,
        }

    def _serialize_regra(r) -> dict:
        return {
            "regra_id": r.regra_id,
            "nome_regra": r.nome_regra,
            "tipo_regra": r.tipo_regra,
            "prioridade_execucao": r.prioridade_execucao,
            "resultado": r.resultado,
            "acoes": r.acoes,
        }

    def _serialize_score(s: ScoreResumo) -> dict:
        return {
            "score_final": s.score_final,
            "score_regulatorio": s.score_regulatorio,
            "score_convenio": s.score_convenio,
            "score_historico": s.score_historico,
            "score_documental": s.score_documental,
            "score_opme": s.score_opme,
            "pesos_usados": s.pesos_usados,
            "fonte_historico": s.fonte_historico,
        }

    opme_serialized = None
    if engine_result.opme_decisao:
        od = engine_result.opme_decisao
        opme_serialized = {
            "items_requeridos": od.items_requeridos,
            "catalogo_sugerido": od.catalogo_sugerido,
            "alertas_opme": od.alertas_opme,
        }

    principal = engine_result.codigo_principal
    artefact = DecisionArtefact(
        decision_id=decision_id,
        episodio_id=episodio_id,
        profile_id=profile_id,
        alias_matched=profile_res.alias_matched,
        codigo_principal=_serialize_codigo(principal) if principal else None,
        codigos_complementares=[_serialize_codigo(c) for c in engine_result.codigos_complementares],
        opme_escolhida=opme_serialized,
        regras_aplicadas=[_serialize_regra(r) for r in engine_result.regras_aplicadas],
        score_resumo=_serialize_score(engine_result.score_resumo),
        payload_documentos=[
            {
                "modelo_id": d.modelo_id,
                "tipo_documento": d.tipo_documento,
                "nome_modelo": d.nome_modelo,
                "template_texto": d.template_texto,
                "blocos_ativos": d.blocos_ativos,
            }
            for d in documentos
        ],
        status_decisao=status,
        alertas=engine_result.alertas,
        bloqueios=engine_result.bloqueios,
        warnings_estruturais=list(dict.fromkeys(all_warnings)),  # deduplica mantendo ordem
        created_at=created_at,
    )

    # ── BLOCO 4c: Persistência ────────────────────────────────────────────────
    try:
        writeDecisionRun(artefact)
    except Exception as e:
        logger.error("writeDecisionRun falhou (não fatal): %s", e)
        all_warnings.append(f"decisao_nao_persistida: {e}")

    logger.info(
        "Motor2 ▸ decision_id=%s profile=%s status=%s score=%.4f alerts=%d blocks=%d",
        decision_id, profile_id, status,
        artefact.score_resumo.get("score_final", 0),
        len(engine_result.alertas),
        len(engine_result.bloqueios),
    )

    return artefact
