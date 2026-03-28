"""
workflow_engine.py — Máquina de estados do Motor 1
Baseada em STATUS_AUTORIZACAO_WORKFLOW_v1.json
Regras:
  - Toda transição é validada contra TRANSICOES_VALIDAS
  - 'arquivado' é terminal e imutável permanentemente
  - Transição inválida levanta WorkflowError (→ HTTP 422)
  - Progressão bloqueada por pendência bloqueante (→ HTTP 409)
"""

from typing import Optional

# ── GRAFO DE TRANSIÇÕES ────────────────────────────────────────────────────────

TRANSICOES_VALIDAS: dict[str, list[str]] = {
    "preenchimento":          ["validacao"],
    "validacao":              ["em_analise", "pendente_complemento"],
    "em_analise":             ["pronto_para_envio"],
    "pendente_complemento":   ["validacao"],
    "pronto_para_envio":      ["enviado"],
    "enviado":                ["autorizado", "negado", "pendente_complemento"],
    "autorizado":             ["faturado"],
    "negado":                 ["arquivado", "recurso_em_preparo"],
    "recurso_em_preparo":     ["recurso_enviado"],
    "recurso_enviado":        ["pendente_retorno_recurso"],
    "pendente_retorno_recurso": ["autorizado", "negado"],
    "faturado":               ["arquivado"],
    "arquivado":              [],          # terminal — hard block permanente
}

ESTADOS_VALIDOS = set(TRANSICOES_VALIDAS.keys())

ESTADOS_TERMINAIS = {"arquivado"}

# Estados que o Motor 1 pode transicionar autonomamente (sem ação humana)
ESTADOS_MOTOR_1 = {
    "preenchimento", "validacao", "em_analise",
    "pendente_complemento", "recurso_enviado",
}

# Estados que exigem ação do convênio (entrada externa)
ESTADOS_AGUARDANDO_CONVENIO = {"enviado", "pendente_retorno_recurso"}

# Estados que disparam alerta crítico imediato
ESTADOS_ALERTA_CRITICO = {"negado"}

# Estados que disparam comunicação ao paciente
ESTADOS_COMUNICACAO_PACIENTE = {"autorizado", "negado"}


# ── ERROS ─────────────────────────────────────────────────────────────────────

class WorkflowError(Exception):
    """Transição de estado inválida."""
    def __init__(self, codigo: str, mensagem: str, detalhes: Optional[dict] = None):
        self.codigo = codigo
        self.mensagem = mensagem
        self.detalhes = detalhes or {}
        super().__init__(mensagem)

    def to_dict(self) -> dict:
        return {
            "erro": self.codigo,
            "mensagem": self.mensagem,
            **self.detalhes,
        }


class BloqueioError(Exception):
    """Progressão bloqueada por pendência ou regra de negócio."""
    def __init__(self, codigo: str, mensagem: str, detalhes: Optional[dict] = None):
        self.codigo = codigo
        self.mensagem = mensagem
        self.detalhes = detalhes or {}
        super().__init__(mensagem)

    def to_dict(self) -> dict:
        return {
            "erro": self.codigo,
            "mensagem": self.mensagem,
            **self.detalhes,
        }


# ── VALIDAÇÕES ────────────────────────────────────────────────────────────────

def validar_transicao(estado_atual: str, estado_destino: str) -> None:
    """
    Levanta WorkflowError se a transição não for permitida.
    Levanta WorkflowError com hard_block_permanente se estado for terminal.
    """
    if estado_atual not in TRANSICOES_VALIDAS:
        raise WorkflowError(
            "estado_desconhecido",
            f"Estado atual '{estado_atual}' não existe no workflow.",
            {"estado_atual": estado_atual},
        )

    if estado_atual in ESTADOS_TERMINAIS:
        raise WorkflowError(
            "hard_block_permanente",
            f"Estado '{estado_atual}' é terminal. Nenhuma transição é permitida.",
            {
                "estado_atual": estado_atual,
                "hard_block_permanente": True,
                "transicoes_permitidas": [],
            },
        )

    permitidas = TRANSICOES_VALIDAS[estado_atual]
    if estado_destino not in permitidas:
        raise WorkflowError(
            "transicao_invalida",
            f"Transição '{estado_atual}' → '{estado_destino}' não é permitida.",
            {
                "estado_atual": estado_atual,
                "estado_destino_solicitado": estado_destino,
                "transicoes_permitidas": permitidas,
            },
        )

    if estado_destino not in ESTADOS_VALIDOS:
        raise WorkflowError(
            "estado_destino_invalido",
            f"Estado destino '{estado_destino}' não existe no workflow.",
            {"estado_destino": estado_destino},
        )


def validar_sem_bloqueio(
    estado_destino: str,
    total_bloqueantes: int,
) -> None:
    """
    Levanta BloqueioError se houver pendências bloqueantes
    e o destino exigir episódio limpo.
    """
    DESTINOS_QUE_EXIGEM_LIMPEZA = {
        "pronto_para_envio",
        "em_analise",
        "enviado",
        "autorizado",
    }
    if estado_destino in DESTINOS_QUE_EXIGEM_LIMPEZA and total_bloqueantes > 0:
        raise BloqueioError(
            "pendencias_bloqueantes",
            f"Existem {total_bloqueantes} pendência(s) bloqueante(s) abertas. "
            f"Resolva-as antes de transicionar para '{estado_destino}'.",
            {
                "estado_destino": estado_destino,
                "total_bloqueantes": total_bloqueantes,
            },
        )


def validar_dados_estado(estado_destino: str, dados_extras: dict) -> None:
    """
    Valida campos obrigatórios específicos por estado de destino.
    """
    if estado_destino == "autorizado":
        if not dados_extras.get("numero_autorizacao"):
            raise WorkflowError(
                "campo_obrigatorio",
                "Transição para 'autorizado' requer 'numero_autorizacao'.",
                {"campo": "numero_autorizacao", "estado_destino": "autorizado"},
            )

    if estado_destino == "negado":
        if not dados_extras.get("motivo_negativa"):
            raise WorkflowError(
                "campo_obrigatorio",
                "Transição para 'negado' requer 'motivo_negativa'.",
                {"campo": "motivo_negativa", "estado_destino": "negado"},
            )


# ── HELPERS ───────────────────────────────────────────────────────────────────

def transicoes_permitidas(estado_atual: str) -> list[str]:
    return TRANSICOES_VALIDAS.get(estado_atual, [])


def e_terminal(estado: str) -> bool:
    return estado in ESTADOS_TERMINAIS


def e_alerta_critico(estado: str) -> bool:
    return estado in ESTADOS_ALERTA_CRITICO
