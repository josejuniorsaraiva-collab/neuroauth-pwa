"""
motor_sherlock/sherlock_engine.py
Motor Sherlock — Camada de raciocínio clínico sobre Motor 2.

Arquitetura Option C (híbrida em 3 fases):
  FASE 1 — Extração: LLM com Sherlock system prompt lê texto clínico livre
            e extrai parâmetros estruturados para o Motor 2.
  FASE 2 — Validação: Motor 2 executa análise determinística contra planilha-mãe.
  FASE 3 — Síntese: LLM sintetiza o artefato do Motor 2 em narrativa clínica.

Ponto de entrada:
    result = run_sherlock(SherlockRequest(...))
"""

import uuid
import logging
from dataclasses import dataclass, field
from typing import Any, Optional
from datetime import datetime, timezone

from motor_sherlock.llm_client import call_llm, call_llm_json, LLMError
from motor2.output_builder import run_motor2

logger = logging.getLogger(__name__)


# ── System Prompt Sherlock (BLOCO 0 — Identidade e missão) ───────────────────

SHERLOCK_SYSTEM_PROMPT = """
Você é o MOTOR SHERLOCK — especialista em análise de autorização cirúrgica de procedimentos neurocirúrgicos no sistema de saúde suplementar brasileiro.

Sua missão é analisar texto clínico livre e extrair parâmetros estruturados para submissão ao Motor 2 de decisão.

## SEUS 12 BLOCOS OPERACIONAIS

### BLOCO 1 — Resolução de Perfil
- Identifique o procedimento cirúrgico principal a partir do texto clínico.
- Resolva o profile_id a partir do nome do procedimento (ex: "artrodese cervical", "microdiscectomia lombar", "artrodese lombar", "artroplastia cervical").
- Se houver ambiguidade, escolha o perfil mais específico com base no contexto clínico.
- Exemplo: "fusão cervical C5-C6 com placa" → profile_texto = "artrodese cervical"

### BLOCO 2 — Protocolo e Nível
- Identifique o número de níveis vertebrais envolvidos (ex: C5-C6 = 1 nível, L4-S1 = 2 níveis).
- Determine o caráter: "eletivo" (padrão) ou "urgencia" (quando texto indica urgência clínica).
- Urgência clínica: déficit neurológico agudo, cauda equina, mielopatia progressiva grave com indicação imediata.

### BLOCO 3 — Combinação de Códigos
- Identifique se há procedimentos complementares além do principal (anestesia, radioscopia, etc.).
- Não liste códigos TUSS — isso é responsabilidade do Motor 2 via planilha-mãe.

### BLOCO 4 — Regras Implícitas
- Observe multiplicadores: "por nível", "por vaso", "por hora" no texto.
- Identifique se OPME será necessária (implantes, parafusos, cage, placa, etc.).
- Se a cirurgia menciona instrumentação: opme_necessita = true.

### BLOCO 5 — OPME
- Liste itens de OPME mencionados no texto (cage, parafusos, placa, fixadores, etc.).
- Para cada item, extraia: descrição, quantidade estimada se mencionada.
- Se marcas específicas forem mencionadas, registre-as como sugestão (não vinculante).

### BLOCO 6 — CID
- Extraia o CID-10 principal do texto clínico.
- Se não informado explicitamente, infira pelo diagnóstico (ex: "estenose lombar" → M48.0).
- Extraia CIDs secundários se mencionados.
- CIDs comuns em neurocirurgia:
  M47.1 - Espondilose com mielopatia
  M47.2 - Espondilose com radiculopatia
  M48.0 - Estenose vertebral
  M51.1 - Degeneração de disco lombar com radiculopatia
  M50.1 - Degeneração de disco cervical com radiculopatia
  G35 - Esclerose múltipla
  S14.1 - Lesão medular cervical (trauma)
  S24.1 - Lesão medular torácica (trauma)
  S34.1 - Lesão medular lombar/sacra (trauma)

### BLOCO 7 — Convênio
- Use o convenio_id fornecido diretamente pelo chamador.
- Não tente inferir o convênio a partir do texto (dado estruturado).

### BLOCO 8 — Urgência / Caráter
- "eletivo" = procedimento programado, sem urgência neurológica imediata.
- "urgencia" = déficit neurológico agudo, síndrome de cauda equina, trauma raquimedular, deterioração rápida.

### BLOCO 9 — Documentação
- Identifique documentos mencionados no texto (laudos, RX, RNM, TC, etc.).
- Registre como observação clínica — o Motor 2 definirá quais documentos são obrigatórios.

### BLOCO 10 — Score e Pesos
- O score é calculado pelo Motor 2 com pesos da planilha-mãe.
- Não calcule scores — apenas extraia dados clínicos relevantes.

### BLOCO 11 — Decisão Final
- A decisão (GO / GO_COM_RESSALVAS / NO_GO) é competência exclusiva do Motor 2.
- Seu papel é extrair dados para que o Motor 2 decida com precisão.

### BLOCO 12 — Output
- Responda SEMPRE em JSON válido, sem markdown extra fora do bloco JSON.
- Siga o schema definido na instrução do usuário.

## REGRAS ABSOLUTAS
- NUNCA invente códigos TUSS ou TUSS específicos — o Motor 2 gerencia isso.
- NUNCA decida sobre autorização — você apenas extrai e sintetiza.
- Se uma informação não estiver no texto, use null (não invente).
- Priorize dados explícitos sobre inferências.
- Em caso de ambiguidade clínica, escolha o perfil mais conservador e registre em observacoes.
""".strip()


SHERLOCK_EXTRACTION_PROMPT = """
Analise o texto clínico abaixo e extraia os parâmetros estruturados para o Motor 2.

TEXTO CLÍNICO:
{texto_clinico}

CONTEXTO ADICIONAL (se fornecido):
- convenio_id: {convenio_id}
- hospital_id: {hospital_id}
- CID sugerido pelo solicitante: {cid_sugerido}
- Dados clínicos extras: {dados_clinicos}

Retorne APENAS um objeto JSON válido com este schema exato:

{{
  "profile_texto": "string — nome do procedimento principal para resolução de alias (ex: artrodese cervical)",
  "niveis": 1,
  "carater": "eletivo",
  "cid_principal": "M47.1",
  "cid_secundarios": [],
  "cbo_executor": null,
  "opme_context": {{
    "necessita_opme": false,
    "itens": []
  }},
  "clinical_context": {{
    "indicacao_clinica": "string — indicação clínica resumida",
    "observacoes": "string — observações adicionais, ambiguidades, etc.",
    "documentos_mencionados": [],
    "urgencia_justificativa": null
  }},
  "confianca_extracao": "alta | media | baixa",
  "ambiguidades": []
}}

Regras:
- niveis: número inteiro de níveis vertebrais (1 se não especificado)
- carater: "urgencia" apenas se houver indicação clínica de urgência neurológica
- cid_principal: CID-10 exato se mencionado; infira se diagnóstico claro, use null se incerto
- opme_context.itens: lista de {{"descricao": "...", "quantidade": 1}} se itens mencionados
- confianca_extracao: "alta" = dados claros; "media" = inferências razoáveis; "baixa" = texto vago/ambíguo
- ambiguidades: lista de strings descrevendo incertezas que o operador deve verificar
"""


SHERLOCK_SYNTHESIS_PROMPT = """
Você é o Motor Sherlock sintetizando a decisão do Motor 2 em linguagem clínica clara.

CONTEXTO DE ENTRADA:
{texto_clinico}

RESULTADO DO MOTOR 2:
- Status: {status}
- Score: {score:.2f}
- Profile resolvido: {profile_id}
- Caráter: {carater}
- Níveis: {niveis}
- CID principal: {cid_principal}
- Alertas: {alertas}
- Bloqueios: {bloqueios}
- Códigos selecionados: {codigos}
- Warnings: {warnings}

INSTRUÇÕES:
Produza uma análise narrativa em português clínico, estruturada em:

1. **Síntese do Caso**: resumo clínico em 2-3 frases.
2. **Decisão de Autorização**: explique o status ({status}) de forma clara para o operador.
3. **Pontos de Atenção**: liste alertas relevantes (se houver).
4. **Documentação Necessária**: indique documentos obrigatórios para o convênio.
5. **Próximos Passos**: ação recomendada para o operador.

Se status for GO: linguagem positiva e confirmadora.
Se status for GO_COM_RESSALVAS: enfatize o que precisa ser resolvido.
Se status for NO_GO: explique claramente o bloqueio e a ação corretiva.

Limite de 400 palavras. Tom profissional, direto, sem jargões desnecessários.
"""


# ── Dataclasses de I/O ────────────────────────────────────────────────────────

@dataclass
class SherlockRequest:
    """Payload de entrada para o Motor Sherlock."""
    texto_clinico: str
    convenio_id: str
    episodio_id: Optional[str] = None
    hospital_id: Optional[str] = None
    cid_sugerido: Optional[str] = None
    dados_clinicos: Optional[dict] = None
    xlsx_path: Optional[str] = None


@dataclass
class ExtractionResult:
    """Resultado da Fase 1 — parâmetros extraídos pelo LLM."""
    profile_texto: str
    niveis: int
    carater: str
    cid_principal: Optional[str]
    cid_secundarios: list[str]
    cbo_executor: Optional[str]
    opme_context: Optional[dict]
    clinical_context: Optional[dict]
    confianca_extracao: str
    ambiguidades: list[str]
    raw_llm_response: str


@dataclass
class SherlockResult:
    """Resultado completo do Motor Sherlock (3 fases)."""
    sherlock_id: str
    episodio_id: Optional[str]
    status: str                           # GO | GO_COM_RESSALVAS | NO_GO | ERRO
    artefato_motor2: dict                 # saída bruta do Motor 2
    interpretacao_sherlock: str           # narrativa clínica (Fase 3)
    parametros_extraidos: dict            # o que o LLM extraiu na Fase 1
    confianca_extracao: str               # alta | media | baixa
    ambiguidades: list[str]               # incertezas identificadas pelo LLM
    score: float
    warnings: list[str]
    erro: Optional[str] = None
    timestamp: str = field(
        default_factory=lambda: datetime.now(timezone.utc).isoformat()
    )

    def to_dict(self) -> dict[str, Any]:
        return {
            "sherlock_id":          self.sherlock_id,
            "episodio_id":          self.episodio_id,
            "status":               self.status,
            "score":                self.score,
            "confianca_extracao":   self.confianca_extracao,
            "ambiguidades":         self.ambiguidades,
            "interpretacao_sherlock": self.interpretacao_sherlock,
            "parametros_extraidos": self.parametros_extraidos,
            "artefato_motor2":      self.artefato_motor2,
            "warnings":             self.warnings,
            "erro":                 self.erro,
            "timestamp":            self.timestamp,
        }


# ── Funções de fase ───────────────────────────────────────────────────────────

def _fase1_extrair_parametros(req: SherlockRequest) -> ExtractionResult:
    """
    FASE 1: LLM extrai parâmetros estruturados do texto clínico.

    Retorna ExtractionResult com todos os campos para o Motor 2.
    Raises LLMError se a extração falhar.
    """
    logger.info("[SHERLOCK] Fase 1 — Extraindo parâmetros de texto clínico")

    user_message = SHERLOCK_EXTRACTION_PROMPT.format(
        texto_clinico=req.texto_clinico,
        convenio_id=req.convenio_id,
        hospital_id=req.hospital_id or "não informado",
        cid_sugerido=req.cid_sugerido or "não informado",
        dados_clinicos=str(req.dados_clinicos) if req.dados_clinicos else "nenhum",
    )

    data = call_llm_json(
        system_prompt=SHERLOCK_SYSTEM_PROMPT,
        user_message=user_message,
        temperature=0.1,
    )

    # Normaliza e valida campos obrigatórios
    profile_texto = data.get("profile_texto") or ""
    if not profile_texto:
        raise LLMError(
            "Extração falhou: 'profile_texto' não identificado no texto clínico. "
            "Forneça o nome do procedimento cirúrgico no texto."
        )

    niveis = int(data.get("niveis") or 1)
    carater = str(data.get("carater") or "eletivo").lower()
    if carater not in ("eletivo", "urgencia"):
        carater = "eletivo"

    opme_ctx = data.get("opme_context") or {}
    if not isinstance(opme_ctx, dict):
        opme_ctx = {}

    clinical_ctx = data.get("clinical_context") or {}
    if not isinstance(clinical_ctx, dict):
        clinical_ctx = {}

    import json
    raw = json.dumps(data, ensure_ascii=False)

    return ExtractionResult(
        profile_texto=profile_texto,
        niveis=niveis,
        carater=carater,
        cid_principal=data.get("cid_principal"),
        cid_secundarios=data.get("cid_secundarios") or [],
        cbo_executor=data.get("cbo_executor"),
        opme_context=opme_ctx if opme_ctx else None,
        clinical_context=clinical_ctx if clinical_ctx else None,
        confianca_extracao=str(data.get("confianca_extracao") or "media"),
        ambiguidades=data.get("ambiguidades") or [],
        raw_llm_response=raw,
    )


def _fase2_validar_motor2(
    req: SherlockRequest,
    params: ExtractionResult,
    sherlock_id: str,
) -> dict:
    """
    FASE 2: Motor 2 valida os parâmetros extraídos contra a planilha-mãe.

    Retorna o artefato do Motor 2 como dict.
    Raises FileNotFoundError se planilha-mãe ausente.
    Raises RuntimeError se Motor 2 falhar inesperadamente.
    """
    logger.info(
        "[SHERLOCK] Fase 2 — Motor 2 validando: profile='%s', niveis=%d, cid='%s'",
        params.profile_texto, params.niveis, params.cid_principal,
    )

    episodio_id = req.episodio_id or f"sherlock-{sherlock_id}"

    # CID principal: usa o sugerido pelo solicitante se o LLM não extraiu nenhum
    cid_principal = params.cid_principal or req.cid_sugerido or "Z99.9"

    artefact = run_motor2(
        episodio_id=episodio_id,
        profile_texto=params.profile_texto,
        convenio_id=req.convenio_id,
        hospital_id=req.hospital_id,
        carater=params.carater,
        niveis=params.niveis,
        cid_principal=cid_principal,
        cid_secundarios=params.cid_secundarios,
        cbo_executor=params.cbo_executor,
        opme_context=params.opme_context,
        clinical_context=params.clinical_context,
        xlsx_path=req.xlsx_path,
    )

    return artefact.to_dict()


def _fase3_sintetizar(
    req: SherlockRequest,
    artefato: dict,
) -> str:
    """
    FASE 3: LLM sintetiza o artefato do Motor 2 em narrativa clínica.

    Retorna string com a interpretação narrativa.
    Em caso de erro do LLM, retorna mensagem de fallback (não levanta exceção).
    """
    logger.info("[SHERLOCK] Fase 3 — Sintetizando resultado do Motor 2")

    status = artefato.get("status", "DESCONHECIDO")
    score = artefato.get("score", {}).get("score_final", 0.0)
    profile_id = artefato.get("profile_id") or "não resolvido"
    carater = artefato.get("carater", "eletivo")
    niveis = artefato.get("niveis", 1)
    cid_principal = artefato.get("cid_principal", "não informado")
    alertas = artefato.get("alertas", [])
    bloqueios = artefato.get("bloqueios", [])
    warnings = artefato.get("warnings", [])

    # Extrai códigos de forma legível
    codigos = artefato.get("codigos_selecionados", {})
    principal = codigos.get("principal")
    cod_str = f"{principal.get('codigo')} - {principal.get('descricao')}" if principal else "não selecionado"

    try:
        narrativa = call_llm(
            system_prompt=SHERLOCK_SYSTEM_PROMPT,
            user_message=SHERLOCK_SYNTHESIS_PROMPT.format(
                texto_clinico=req.texto_clinico[:1000],  # trunca para economizar tokens
                status=status,
                score=float(score),
                profile_id=profile_id,
                carater=carater,
                niveis=niveis,
                cid_principal=cid_principal,
                alertas=str(alertas[:5]) if alertas else "nenhum",
                bloqueios=str(bloqueios[:3]) if bloqueios else "nenhum",
                codigos=cod_str,
                warnings=str(warnings[:5]) if warnings else "nenhum",
            ),
            temperature=0.3,
            max_tokens=1024,
        )
        return narrativa.strip()
    except LLMError as e:
        logger.warning("[SHERLOCK] Fase 3 falhou (LLM): %s — usando fallback", e)
        return (
            f"[Síntese automática indisponível]\n\n"
            f"Status Motor 2: **{status}** | Score: {score:.2f}\n"
            f"Profile: {profile_id} | CID: {cid_principal}\n"
            f"Alertas: {len(alertas)} | Bloqueios: {len(bloqueios)}\n\n"
            f"Consulte o artefato_motor2 para detalhes completos."
        )


# ── Ponto de entrada principal ────────────────────────────────────────────────

def run_sherlock(req: SherlockRequest) -> SherlockResult:
    """
    Executa o pipeline completo do Motor Sherlock (3 fases).

    Args:
        req: SherlockRequest com texto clínico e contexto.

    Returns:
        SherlockResult com artefato Motor 2 + interpretação clínica + metadados.

    O método NÃO levanta exceções — erros são capturados e retornados
    em SherlockResult.status = "ERRO" com SherlockResult.erro preenchido.
    """
    sherlock_id = str(uuid.uuid4())
    logger.info(
        "[SHERLOCK] Iniciando run_sherlock id=%s convenio=%s",
        sherlock_id, req.convenio_id,
    )

    # ── FASE 1: Extração ──────────────────────────────────────────────────────
    try:
        params = _fase1_extrair_parametros(req)
    except LLMError as e:
        logger.error("[SHERLOCK] Fase 1 falhou: %s", e)
        return SherlockResult(
            sherlock_id=sherlock_id,
            episodio_id=req.episodio_id,
            status="ERRO",
            artefato_motor2={},
            interpretacao_sherlock="",
            parametros_extraidos={},
            confianca_extracao="baixa",
            ambiguidades=[],
            score=0.0,
            warnings=[],
            erro=f"Fase 1 (extração LLM) falhou: {e}",
        )

    params_dict = {
        "profile_texto":      params.profile_texto,
        "niveis":             params.niveis,
        "carater":            params.carater,
        "cid_principal":      params.cid_principal,
        "cid_secundarios":    params.cid_secundarios,
        "cbo_executor":       params.cbo_executor,
        "opme_context":       params.opme_context,
        "clinical_context":   params.clinical_context,
        "confianca_extracao": params.confianca_extracao,
        "ambiguidades":       params.ambiguidades,
    }

    # ── FASE 2: Validação Motor 2 ─────────────────────────────────────────────
    try:
        artefato = _fase2_validar_motor2(req, params, sherlock_id)
    except FileNotFoundError as e:
        logger.error("[SHERLOCK] Fase 2 — planilha não encontrada: %s", e)
        return SherlockResult(
            sherlock_id=sherlock_id,
            episodio_id=req.episodio_id,
            status="ERRO",
            artefato_motor2={},
            interpretacao_sherlock="",
            parametros_extraidos=params_dict,
            confianca_extracao=params.confianca_extracao,
            ambiguidades=params.ambiguidades,
            score=0.0,
            warnings=[],
            erro=f"Fase 2 (Motor 2) falhou — planilha-mãe não encontrada: {e}",
        )
    except Exception as e:
        logger.error("[SHERLOCK] Fase 2 — erro inesperado: %s", e, exc_info=True)
        return SherlockResult(
            sherlock_id=sherlock_id,
            episodio_id=req.episodio_id,
            status="ERRO",
            artefato_motor2={},
            interpretacao_sherlock="",
            parametros_extraidos=params_dict,
            confianca_extracao=params.confianca_extracao,
            ambiguidades=params.ambiguidades,
            score=0.0,
            warnings=[],
            erro=f"Fase 2 (Motor 2) falhou: {type(e).__name__}: {e}",
        )

    # ── FASE 3: Síntese narrativa ─────────────────────────────────────────────
    interpretacao = _fase3_sintetizar(req, artefato)

    # ── Monta resultado final ─────────────────────────────────────────────────
    status = artefato.get("status", "DESCONHECIDO")
    score = artefato.get("score", {}).get("score_final", 0.0)
    warnings = artefato.get("warnings", [])
    if params.ambiguidades:
        warnings = list(warnings) + [
            f"sherlock_ambiguidade: {a}" for a in params.ambiguidades
        ]

    logger.info(
        "[SHERLOCK] Concluído id=%s status=%s score=%.2f confianca=%s",
        sherlock_id, status, float(score), params.confianca_extracao,
    )

    return SherlockResult(
        sherlock_id=sherlock_id,
        episodio_id=req.episodio_id,
        status=status,
        artefato_motor2=artefato,
        interpretacao_sherlock=interpretacao,
        parametros_extraidos=params_dict,
        confianca_extracao=params.confianca_extracao,
        ambiguidades=params.ambiguidades,
        score=float(score),
        warnings=warnings,
    )
