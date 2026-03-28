"""
pendencia_engine.py — Motor 1: Orquestração de Pendências
Coordena criação, deduplicação e resolução de pendências.
Não tem efeitos colaterais além de chamar episode_store.
"""

import uuid
from typing import Optional

from motor1 import episode_store as store
from motor1.validator_engine import PendenciaResult


def _novo_id() -> str:
    return str(uuid.uuid4())


def criar_pendencias_do_resultado(
    id_episodio: str,
    pendencias: list[PendenciaResult],
) -> list[dict]:
    """
    Persiste pendências novas evitando duplicação por campo_afetado.
    Retorna lista de pendências criadas (apenas as novas).
    """
    campos_abertos = store.campos_com_pendencia_aberta(id_episodio)
    criadas = []

    for p in pendencias:
        # Não duplicar pendência no mesmo campo se já há uma aberta
        if p.campo_afetado and p.campo_afetado in campos_abertos:
            continue

        nova = store.create_pendencia(
            id_pendencia=_novo_id(),
            id_episodio=id_episodio,
            tipo=p.tipo,
            descricao=p.descricao,
            campo_afetado=p.campo_afetado,
            bloqueia_envio=p.bloqueia_envio,
            severidade=p.severidade,
        )
        if p.campo_afetado:
            campos_abertos.add(p.campo_afetado)
        criadas.append(nova)

    return criadas


def resolver(
    id_episodio: str,
    id_pendencia: str,
    resolucao: str,
    resolvido_por: str,
) -> dict:
    """
    Resolve uma pendência. Valida que pertence ao episódio.
    Levanta ValueError se não encontrada ou já resolvida.
    """
    p = store.get_pendencia(id_pendencia)

    if not p:
        raise ValueError(f"Pendência '{id_pendencia}' não encontrada.")

    if p["id_episodio"] != id_episodio:
        raise ValueError(
            f"Pendência '{id_pendencia}' não pertence ao episódio '{id_episodio}'."
        )

    if p["status"] == "resolvida":
        raise ValueError(
            f"Pendência '{id_pendencia}' já está resolvida."
        )

    return store.resolve_pendencia(id_pendencia, resolucao, resolvido_por)


def listar(id_episodio: str) -> dict:
    """Retorna todas as pendências do episódio com contadores."""
    todas = store.get_pendencias(id_episodio)
    abertas    = [p for p in todas if p["status"] == "aberta"]
    bloqueantes = [p for p in abertas if p["bloqueia_envio"]]
    resolvidas  = [p for p in todas if p["status"] == "resolvida"]

    return {
        "total":             len(todas),
        "abertas":           len(abertas),
        "bloqueantes":       len(bloqueantes),
        "resolvidas":        len(resolvidas),
        "pode_revalidar":    len(bloqueantes) == 0,
        "pendencias":        todas,
    }
