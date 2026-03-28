"""
motor2/resolver.py — BLOCO 2
Resolução do contexto do caso clínico a partir da Planilha-Mãe.

Funções:
  resolveProfileFromAlias()   — texto → profile_id via 02_PROC_ALIAS
  hydrateProfileMaster()      — profile_id → dados completos de 01_PROC_MESTRE
  resolveConvenio()           — convenio_id → dados de 03_CONVENIOS
  resolveHospital()           — hospital_id → dados de 04_HOSPITAIS
  resolveCIDRules()           — valida cid_principal contra 07_CIDS_PERMITIDOS
  resolveCBORules()           — valida cbo_executor contra 08_CBOS_PERMITIDOS

Ordem de resolução de profile_id:
  1. match exato em alias_texto (normalizado)
  2. match de substring (profile_id contido no texto)
  3. maior prioridade_match entre candidatos

Tratamento de ACDF_1_NIVEL:
  Se profile_id resolvido não existe em PROC_MESTRE → warning estruturado,
  motor continua em modo degradado controlado (GO_COM_RESSALVAS).
"""

import re
import logging
from typing import Optional
from dataclasses import dataclass, field

from motor2.sheet_reader import SheetData

logger = logging.getLogger(__name__)


def _normalize_text(text: str) -> str:
    """Normaliza texto para matching: lowercase, sem acentos básicos, sem pontuação."""
    text = text.lower().strip()
    # Remove acentos básicos do português
    replacements = {
        "ã": "a", "á": "a", "â": "a", "à": "a",
        "é": "e", "ê": "e", "è": "e",
        "í": "i", "î": "i",
        "ó": "o", "ô": "o", "õ": "o",
        "ú": "u", "û": "u",
        "ç": "c",
    }
    for src, dst in replacements.items():
        text = text.replace(src, dst)
    # Remove pontuação exceto hífen e espaço
    text = re.sub(r"[^\w\s\-]", "", text)
    return text


@dataclass
class ProfileResolution:
    """Resultado da resolução de perfil."""
    profile_id: Optional[str]
    alias_matched: Optional[str]       # alias_texto que gerou o match
    match_type: str                    # exact | substring | no_match
    prioridade_match: int              # da linha de alias
    profile_master: Optional[dict]     # dados de 01_PROC_MESTRE (None se ausente)
    warnings: list[str] = field(default_factory=list)

    @property
    def resolved(self) -> bool:
        return self.profile_id is not None

    @property
    def master_present(self) -> bool:
        return self.profile_master is not None


@dataclass
class CIDResolution:
    """Resultado da validação de CID."""
    cid: str
    tipo_relacao: Optional[str]   # preferencial | permitido | alerta | None (não encontrado)
    is_valid: bool
    requires_justification: bool  # True quando tipo_relacao == 'alerta'
    warnings: list[str] = field(default_factory=list)


@dataclass
class CBOResolution:
    """Resultado da validação de CBO."""
    cbo: Optional[str]
    tipo_relacao: Optional[str]   # preferencial | permitido | None
    is_valid: bool
    warnings: list[str] = field(default_factory=list)


def resolveProfileFromAlias(
    texto: str,
    alias_sheet: SheetData,
    proc_mestre_sheet: SheetData,
) -> ProfileResolution:
    """
    Resolve profile_id a partir de texto livre (nome do procedimento).

    Estratégia:
      1. Normaliza texto de entrada
      2. Para cada alias ativo, normaliza alias_texto
      3. Match exato → seleciona de imediato
      4. Match de substring (alias contido no texto ou texto contido no alias)
      5. Ordena candidatos por prioridade_match DESC → retorna o melhor
      6. Se nenhum → profile_id=None, warnings
    """
    texto_norm = _normalize_text(texto)
    candidates: list[tuple[int, dict]] = []  # (prioridade_match, row)

    active_aliases = [r for r in alias_sheet.rows if r.get("ativo") is not False and r.get("profile_id")]

    for row in active_aliases:
        alias = _normalize_text(str(row.get("alias_texto", "")))
        if not alias:
            continue
        prioridade = int(row.get("prioridade_match") or 0)

        if alias == texto_norm:
            # Match exato — retorna imediatamente
            return _build_resolution(row, "exact", prioridade, proc_mestre_sheet)

        if alias in texto_norm or texto_norm in alias:
            candidates.append((prioridade, row))

    if not candidates:
        logger.warning("resolveProfileFromAlias: nenhum match para '%s'", texto)
        return ProfileResolution(
            profile_id=None,
            alias_matched=None,
            match_type="no_match",
            prioridade_match=0,
            profile_master=None,
            warnings=[f"profile_not_found: nenhum alias corresponde ao texto '{texto}'"],
        )

    # Melhor candidato por prioridade
    candidates.sort(key=lambda x: x[0], reverse=True)
    best_prioridade, best_row = candidates[0]
    return _build_resolution(best_row, "substring", best_prioridade, proc_mestre_sheet)


def _build_resolution(
    alias_row: dict,
    match_type: str,
    prioridade: int,
    proc_mestre_sheet: SheetData,
) -> ProfileResolution:
    profile_id = alias_row["profile_id"]
    warnings: list[str] = []

    # Hidratar do master
    master = hydrateProfileMaster(profile_id, proc_mestre_sheet)
    if master is None:
        warnings.append(
            f"profile_master_missing: profile_id '{profile_id}' resolvido via alias "
            f"mas ausente em 01_PROC_MESTRE — motor continua em modo degradado controlado"
        )
        logger.warning("[RESOLVER] %s", warnings[-1])

    return ProfileResolution(
        profile_id=profile_id,
        alias_matched=alias_row.get("alias_texto"),
        match_type=match_type,
        prioridade_match=prioridade,
        profile_master=master,
        warnings=warnings,
    )


def hydrateProfileMaster(
    profile_id: str,
    proc_mestre_sheet: SheetData,
) -> Optional[dict]:
    """
    Retorna os dados completos do perfil de 01_PROC_MESTRE.
    Retorna None (não levanta exceção) se profile_id ausente.
    """
    return proc_mestre_sheet.find_one(profile_id=profile_id)


def resolveConvenio(
    convenio_id: str,
    convenios_sheet: SheetData,
) -> Optional[dict]:
    """
    Retorna dados do convênio de 03_CONVENIOS.
    Primeiro busca match exato, depois 'GLOBAL', depois None com warning.
    """
    row = convenios_sheet.find_one(convenio_id=convenio_id)
    if row:
        return row
    # Fallback GLOBAL (se existir)
    row = convenios_sheet.find_one(convenio_id="GLOBAL")
    if row:
        logger.warning("[RESOLVER] convenio '%s' não encontrado — usando GLOBAL", convenio_id)
        return row
    logger.warning("[RESOLVER] convenio '%s' não encontrado e sem GLOBAL", convenio_id)
    return None


def resolveHospital(
    hospital_id: Optional[str],
    hospitais_sheet: SheetData,
) -> Optional[dict]:
    """Retorna dados do hospital de 04_HOSPITAIS. None se não encontrado."""
    if not hospital_id:
        return None
    row = hospitais_sheet.find_one(hospital_id=hospital_id)
    if not row:
        logger.warning("[RESOLVER] hospital_id '%s' não encontrado em 04_HOSPITAIS", hospital_id)
    return row


def resolveCIDRules(
    profile_id: str,
    cid_principal: str,
    cids_sheet: SheetData,
) -> CIDResolution:
    """
    Valida o CID principal contra 07_CIDS_PERMITIDOS para o profile.

    tipo_relacao:
      preferencial → CID ideal, nenhum alerta
      permitido    → aceito sem alerta
      alerta       → aceito mas exige justificativa reforçada
      None         → CID não mapeado para o profile → warning

    Se não há CIDs mapeados para o profile (sheet vazia ou sem linhas do profile),
    aceita o CID como permitido com warning.
    """
    warnings: list[str] = []
    profile_cids = cids_sheet.filter(profile_id=profile_id, ativo=True)

    if not profile_cids:
        warnings.append(
            f"cid_rules_missing: nenhum CID mapeado para profile_id '{profile_id}' "
            f"em 07_CIDS_PERMITIDOS — CID '{cid_principal}' aceito sem validação"
        )
        return CIDResolution(
            cid=cid_principal,
            tipo_relacao=None,
            is_valid=True,
            requires_justification=False,
            warnings=warnings,
        )

    matched = next(
        (r for r in profile_cids if r.get("cid", "").upper() == cid_principal.upper()),
        None,
    )

    if not matched:
        allowed_cids = [r["cid"] for r in profile_cids]
        warnings.append(
            f"cid_nao_mapeado: CID '{cid_principal}' não mapeado para profile '{profile_id}'. "
            f"CIDs permitidos: {allowed_cids}"
        )
        return CIDResolution(
            cid=cid_principal,
            tipo_relacao=None,
            is_valid=False,
            requires_justification=True,
            warnings=warnings,
        )

    tipo = matched.get("tipo_relacao", "permitido")
    requires_justification = tipo == "alerta"

    if tipo == "alerta":
        warnings.append(
            f"cid_alerta: CID '{cid_principal}' requer justificativa reforçada — "
            f"{matched.get('observacao', '')}"
        )

    return CIDResolution(
        cid=cid_principal,
        tipo_relacao=tipo,
        is_valid=True,
        requires_justification=requires_justification,
        warnings=warnings,
    )


def resolveCBORules(
    profile_id: str,
    cbo_executor: Optional[str],
    convenio_id: Optional[str],
    cbos_sheet: SheetData,
) -> CBOResolution:
    """
    Valida o CBO do executor contra 08_CBOS_PERMITIDOS.
    Se cbo_executor não informado → aceito com warning.
    """
    warnings: list[str] = []

    if not cbo_executor:
        warnings.append("cbo_nao_informado: cbo_executor ausente — validação de CBO ignorada")
        return CBOResolution(cbo=None, tipo_relacao=None, is_valid=True, warnings=warnings)

    # Normaliza CBO (pode vir como int ou string)
    cbo_norm = str(cbo_executor).strip()

    profile_cbos = cbos_sheet.filter(profile_id=profile_id, ativo=True)

    if not profile_cbos:
        warnings.append(
            f"cbo_rules_missing: nenhum CBO mapeado para '{profile_id}' — "
            f"CBO '{cbo_norm}' aceito sem validação"
        )
        return CBOResolution(cbo=cbo_norm, tipo_relacao=None, is_valid=True, warnings=warnings)

    # Busca match por CBO (pode ser int na planilha)
    matched = next(
        (r for r in profile_cbos if str(r.get("cbo", "")).strip() == cbo_norm),
        None,
    )

    if not matched:
        allowed = [str(r.get("cbo")) for r in profile_cbos]
        warnings.append(
            f"cbo_nao_permitido: CBO '{cbo_norm}' não mapeado para '{profile_id}'. "
            f"CBOs permitidos: {allowed}"
        )
        return CBOResolution(cbo=cbo_norm, tipo_relacao=None, is_valid=False, warnings=warnings)

    return CBOResolution(
        cbo=cbo_norm,
        tipo_relacao=matched.get("tipo_relacao", "permitido"),
        is_valid=True,
        warnings=warnings,
    )
