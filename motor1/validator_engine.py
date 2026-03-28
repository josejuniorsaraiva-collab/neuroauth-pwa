"""
validator_engine.py — Motor 1: Pipeline de Validação (4 checks)
Verificado nos Cenários A e B.

Checks executados em sequência:
  A — Completude de campos obrigatórios (inclui OPME)
  B — Compatibilidade clínica: TUSS × CID-10
  C — Regulatório: CRM, CNES, validade carteirinha
  D — Cobertura do convênio: TUSS no rol

Retorna lista de PendenciaResult.
Sem efeitos colaterais — não grava no banco.
"""

from dataclasses import dataclass
from typing import Optional
from datetime import date


# ── TABELAS DE REFERÊNCIA (hardcoded no MVP — banco em v1.1) ─────────────────

# TUSS → CIDs aceitos (subconjunto operacional: neurocirurgia + coluna)
TUSS_CID_COMPAT: dict[str, list[str]] = {
    "40803015": ["M51.1", "M48.0", "M47.8", "M43.1", "M54.4", "S32.0", "M51.0", "M51.2"],
    "40803023": ["M50.1", "M50.2", "M50.0", "M47.8", "S12.0", "M43.3", "M43.2"],
    "40801011": ["M51.1", "M51.0", "M51.2", "M54.4", "M48.0"],  # microdiscectomia
    "40804011": ["M48.0", "M47.8", "M51.1", "G95.0", "M43.0"],  # laminectomia
    "40701014": ["G35", "G93.6", "C71.0", "C71.1", "C71.2", "C71.9",
                 "I60.0", "I60.9", "I61.0", "I61.9", "I67.1", "G91.0"],  # craniotomia
    "40702022": ["I65.0", "I65.1", "I65.2", "I66.0", "I66.1",
                 "Q28.3", "I67.1", "I60.9"],  # angiografia cerebral
}

# CNES credenciados na Unimed Cariri (MVP)
CNES_CREDENCIADOS_UNIMED_CARIRI: set[str] = {
    "2330420",   # Hospital São Antônio — Barbalha/CE
    "2315820",   # Hospital Regional do Cariri — Crato/CE
    "7003527",   # Hospital Maternidade São Lucas — Juazeiro do Norte/CE
    "2334520",   # UNI-Cariri Hospital — Juazeiro do Norte/CE
}

# TUSS cobertos pela Unimed Cariri (MVP)
ROL_UNIMED_CARIRI: set[str] = {
    "40803015",  # Artrodese lombar
    "40803023",  # Artrodese cervical
    "40801011",  # Microdiscectomia
    "40804011",  # Laminectomia
    "40701014",  # Craniotomia
    "40702022",  # Angiografia cerebral
    "40702030",  # Embolização cerebral
    "40801020",  # Discectomia cervical
    "40804020",  # Foraminotomia
}

# Campos obrigatórios por (bloco, campo)
CAMPOS_OBRIGATORIOS: list[tuple[str, str]] = [
    ("identificacao_caso", "tipo_atendimento"),
    ("paciente",           "carteirinha"),
    ("paciente",           "nome"),
    ("paciente",           "cpf"),
    ("medico",             "crm"),
    ("hospital",           "cnes"),
    ("convenio",           "id_convenio"),
    ("convenio",           "codigo_tiss"),
    ("procedimento_principal", "codigo_tuss"),
    ("procedimento_principal", "cid_principal"),
    ("procedimento_principal", "indicacao_clinica"),
]

INDICACAO_MINIMA_CHARS = 50


# ── RESULTADO DE PENDÊNCIA ────────────────────────────────────────────────────

@dataclass
class PendenciaResult:
    tipo: str
    descricao: str
    campo_afetado: Optional[str]
    bloqueia_envio: bool
    severidade: str  # "critica" | "alta" | "media"


# ── CHECKS ───────────────────────────────────────────────────────────────────

def _check_a_completude(dados: dict) -> list[PendenciaResult]:
    """Verifica campos obrigatórios e completude de OPME."""
    pendencias: list[PendenciaResult] = []

    for bloco, campo in CAMPOS_OBRIGATORIOS:
        val = dados.get(bloco, {}).get(campo, "")
        if not val or (isinstance(val, str) and not val.strip()):
            pendencias.append(PendenciaResult(
                tipo="documentacao_incompleta",
                descricao=f"Campo obrigatório ausente: {bloco}.{campo}",
                campo_afetado=f"{bloco}.{campo}",
                bloqueia_envio=True,
                severidade="critica",
            ))

    # Indicação clínica — mínimo de chars
    indicacao = dados.get("procedimento_principal", {}).get("indicacao_clinica", "")
    if indicacao and len(indicacao.strip()) < INDICACAO_MINIMA_CHARS:
        pendencias.append(PendenciaResult(
            tipo="informacao_clinica_insuficiente",
            descricao=(
                f"Indicação clínica insuficiente ({len(indicacao.strip())} chars). "
                f"Mínimo: {INDICACAO_MINIMA_CHARS} chars com descrição clínica objetiva."
            ),
            campo_afetado="procedimento_principal.indicacao_clinica",
            bloqueia_envio=True,
            severidade="alta",
        ))

    # OPME
    opme = dados.get("opme", {})
    if opme.get("necessita_opme") is True:
        itens = opme.get("itens", [])
        if not itens:
            pendencias.append(PendenciaResult(
                tipo="opme_nao_autorizada",
                descricao="OPME marcado como necessário, mas itens[] está vazio. Informe os itens com código ANVISA.",
                campo_afetado="opme.itens",
                bloqueia_envio=True,
                severidade="critica",
            ))
        else:
            for i, item in enumerate(itens):
                if not item.get("codigo_anvisa", "").strip():
                    pendencias.append(PendenciaResult(
                        tipo="opme_nao_autorizada",
                        descricao=f"Item OPME [{i+1}] sem código ANVISA. Obrigatório para autorização.",
                        campo_afetado=f"opme.itens[{i}].codigo_anvisa",
                        bloqueia_envio=True,
                        severidade="critica",
                    ))

        if not opme.get("justificativa_clinica", "").strip():
            pendencias.append(PendenciaResult(
                tipo="informacao_clinica_insuficiente",
                descricao="Justificativa clínica do OPME ausente. Obrigatória quando necessita_opme = true.",
                campo_afetado="opme.justificativa_clinica",
                bloqueia_envio=True,
                severidade="alta",
            ))

    return pendencias


def _check_b_clinico(dados: dict) -> list[PendenciaResult]:
    """Verifica compatibilidade TUSS × CID-10."""
    pendencias: list[PendenciaResult] = []

    tuss = dados.get("procedimento_principal", {}).get("codigo_tuss", "")
    cid  = dados.get("procedimento_principal", {}).get("cid_principal", "")

    if tuss and cid and tuss in TUSS_CID_COMPAT:
        aceitos = TUSS_CID_COMPAT[tuss]
        # Match direto ou por prefixo (ex: "M51" cobre "M51.1")
        if not any(cid.startswith(aceito.split(".")[0]) or cid == aceito for aceito in aceitos):
            pendencias.append(PendenciaResult(
                tipo="cid_incompativel",
                descricao=(
                    f"CID-10 '{cid}' incompatível com procedimento TUSS {tuss}. "
                    f"CIDs aceitos: {', '.join(aceitos[:5])}{'...' if len(aceitos) > 5 else ''}."
                ),
                campo_afetado="procedimento_principal.cid_principal",
                bloqueia_envio=True,
                severidade="critica",
            ))

    return pendencias


def _check_c_regulatorio(dados: dict) -> list[PendenciaResult]:
    """Verifica CRM, CNES e validade da carteirinha."""
    pendencias: list[PendenciaResult] = []

    # CRM
    crm = dados.get("medico", {}).get("crm", "")
    if crm:
        crm_valido = (
            crm.upper().startswith("CRM/")
            and len(crm) >= 8
            and any(c.isdigit() for c in crm)
        )
        if not crm_valido:
            pendencias.append(PendenciaResult(
                tipo="dados_beneficiario_invalidos",
                descricao=f"CRM em formato inválido: '{crm}'. Formato esperado: CRM/UF 12345.",
                campo_afetado="medico.crm",
                bloqueia_envio=True,
                severidade="critica",
            ))

    # CNES
    cnes = dados.get("hospital", {}).get("cnes", "")
    id_convenio = dados.get("convenio", {}).get("id_convenio", "")
    if cnes and id_convenio == "UNIMED_CARIRI":
        if cnes not in CNES_CREDENCIADOS_UNIMED_CARIRI:
            pendencias.append(PendenciaResult(
                tipo="conflito_cobertura",
                descricao=(
                    f"Hospital com CNES {cnes} não está na rede credenciada da Unimed Cariri. "
                    "Verifique o hospital ou solicite credenciamento."
                ),
                campo_afetado="hospital.cnes",
                bloqueia_envio=True,
                severidade="critica",
            ))

    # Validade da carteirinha
    validade_str = dados.get("paciente", {}).get("validade_carteirinha", "")
    if validade_str:
        try:
            validade_dt = date.fromisoformat(validade_str)
            if validade_dt < date.today():
                pendencias.append(PendenciaResult(
                    tipo="dados_beneficiario_invalidos",
                    descricao=f"Carteirinha do beneficiário vencida em {validade_str}. Contate o convênio.",
                    campo_afetado="paciente.validade_carteirinha",
                    bloqueia_envio=True,
                    severidade="critica",
                ))
        except ValueError:
            pendencias.append(PendenciaResult(
                tipo="dados_beneficiario_invalidos",
                descricao=f"Data de validade da carteirinha inválida: '{validade_str}'. Use formato YYYY-MM-DD.",
                campo_afetado="paciente.validade_carteirinha",
                bloqueia_envio=True,
                severidade="alta",
            ))

    return pendencias


def _check_d_cobertura(dados: dict) -> list[PendenciaResult]:
    """Verifica se o procedimento TUSS está coberto pelo convênio."""
    pendencias: list[PendenciaResult] = []

    tuss       = dados.get("procedimento_principal", {}).get("codigo_tuss", "")
    id_convenio = dados.get("convenio", {}).get("id_convenio", "")

    if tuss and id_convenio == "UNIMED_CARIRI":
        if tuss not in ROL_UNIMED_CARIRI:
            pendencias.append(PendenciaResult(
                tipo="procedimento_nao_coberto",
                descricao=(
                    f"Procedimento TUSS {tuss} não consta no rol de cobertura da Unimed Cariri. "
                    "Verifique o código ou solicite autorização prévia de cobertura."
                ),
                campo_afetado="procedimento_principal.codigo_tuss",
                bloqueia_envio=True,
                severidade="critica",
            ))

    return pendencias


# ── PIPELINE PRINCIPAL ────────────────────────────────────────────────────────

class ResultadoValidacao:
    def __init__(self):
        self.check_a: list[PendenciaResult] = []
        self.check_b: list[PendenciaResult] = []
        self.check_c: list[PendenciaResult] = []
        self.check_d: list[PendenciaResult] = []

    @property
    def todas_pendencias(self) -> list[PendenciaResult]:
        return self.check_a + self.check_b + self.check_c + self.check_d

    @property
    def aprovado(self) -> bool:
        return len(self.todas_pendencias) == 0

    def to_dict(self) -> dict:
        return {
            "check_a_completude":   "PASS" if not self.check_a else "FAIL",
            "check_b_clinico":      "PASS" if not self.check_b else "FAIL",
            "check_c_regulatorio":  "PASS" if not self.check_c else "FAIL",
            "check_d_cobertura":    "PASS" if not self.check_d else "FAIL",
            "total_pendencias":     len(self.todas_pendencias),
            "aprovado":             self.aprovado,
        }


def executar(dados: dict) -> ResultadoValidacao:
    """
    Executa os 4 checks em sequência.
    Retorna ResultadoValidacao com pendências encontradas.
    Sem efeitos colaterais.
    """
    resultado = ResultadoValidacao()
    resultado.check_a = _check_a_completude(dados)
    resultado.check_b = _check_b_clinico(dados)
    resultado.check_c = _check_c_regulatorio(dados)
    resultado.check_d = _check_d_cobertura(dados)
    return resultado
