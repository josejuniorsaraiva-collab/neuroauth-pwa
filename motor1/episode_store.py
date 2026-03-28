"""
episode_store.py — Persistência SQLite do Motor 1
Mantém: episódios, pendências, timeline_eventos
Todas as escritas são append-only na timeline.
"""

import sqlite3
import json
import os
from contextlib import contextmanager
from datetime import datetime, timezone
from typing import Optional

DB_PATH = os.environ.get("DB_PATH", "/tmp/neuroauth.db")


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


@contextmanager
def get_conn():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    try:
        yield conn
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def init_db():
    with get_conn() as conn:
        conn.executescript("""
            CREATE TABLE IF NOT EXISTS episodios (
                id_episodio   TEXT PRIMARY KEY,
                request_id    TEXT UNIQUE NOT NULL,
                estado_atual  TEXT NOT NULL DEFAULT 'preenchimento',
                dados         TEXT NOT NULL,
                criado_em     TEXT NOT NULL,
                atualizado_em TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS timeline (
                id_evento     TEXT PRIMARY KEY,
                id_episodio   TEXT NOT NULL,
                timestamp     TEXT NOT NULL,
                tipo          TEXT NOT NULL,
                origem        TEXT NOT NULL,
                estado_antes  TEXT,
                estado_depois TEXT,
                dados         TEXT,
                FOREIGN KEY (id_episodio) REFERENCES episodios(id_episodio)
            );

            CREATE TABLE IF NOT EXISTS pendencias (
                id_pendencia  TEXT PRIMARY KEY,
                id_episodio   TEXT NOT NULL,
                tipo          TEXT NOT NULL,
                descricao     TEXT NOT NULL,
                campo_afetado TEXT,
                bloqueia_envio INTEGER NOT NULL DEFAULT 1,
                severidade    TEXT NOT NULL DEFAULT 'critica',
                status        TEXT NOT NULL DEFAULT 'aberta',
                criada_em     TEXT NOT NULL,
                resolvida_em  TEXT,
                resolucao     TEXT,
                resolvido_por TEXT,
                FOREIGN KEY (id_episodio) REFERENCES episodios(id_episodio)
            );

            CREATE INDEX IF NOT EXISTS idx_timeline_episodio
                ON timeline(id_episodio);
            CREATE INDEX IF NOT EXISTS idx_pendencias_episodio
                ON pendencias(id_episodio);
        """)


# ── EPISÓDIO ──────────────────────────────────────────────────────────────────

def get_by_request_id(request_id: str) -> Optional[dict]:
    with get_conn() as conn:
        row = conn.execute(
            "SELECT * FROM episodios WHERE request_id = ?", (request_id,)
        ).fetchone()
        return dict(row) if row else None


def get_episodio(id_episodio: str) -> Optional[dict]:
    with get_conn() as conn:
        row = conn.execute(
            "SELECT * FROM episodios WHERE id_episodio = ?", (id_episodio,)
        ).fetchone()
        return dict(row) if row else None


def create_episodio(id_episodio: str, request_id: str, dados: dict) -> dict:
    agora = _now()
    with get_conn() as conn:
        conn.execute(
            """INSERT INTO episodios
               (id_episodio, request_id, estado_atual, dados, criado_em, atualizado_em)
               VALUES (?, ?, 'preenchimento', ?, ?, ?)""",
            (id_episodio, request_id, json.dumps(dados), agora, agora),
        )
    return get_episodio(id_episodio)


def update_estado(id_episodio: str, novo_estado: str):
    with get_conn() as conn:
        conn.execute(
            "UPDATE episodios SET estado_atual = ?, atualizado_em = ? WHERE id_episodio = ?",
            (novo_estado, _now(), id_episodio),
        )


def update_dados(id_episodio: str, dados: dict):
    with get_conn() as conn:
        conn.execute(
            "UPDATE episodios SET dados = ?, atualizado_em = ? WHERE id_episodio = ?",
            (json.dumps(dados), _now(), id_episodio),
        )


def list_episodios(limite: int = 50) -> list:
    with get_conn() as conn:
        rows = conn.execute(
            """SELECT id_episodio, request_id, estado_atual, criado_em, atualizado_em
               FROM episodios ORDER BY criado_em DESC LIMIT ?""",
            (limite,),
        ).fetchall()
        return [dict(r) for r in rows]


# ── TIMELINE ─────────────────────────────────────────────────────────────────

def append_evento(
    id_episodio: str,
    id_evento: str,
    tipo: str,
    origem: str,
    estado_antes: Optional[str] = None,
    estado_depois: Optional[str] = None,
    dados: Optional[dict] = None,
):
    with get_conn() as conn:
        conn.execute(
            """INSERT INTO timeline
               (id_evento, id_episodio, timestamp, tipo, origem, estado_antes, estado_depois, dados)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?)""",
            (
                id_evento,
                id_episodio,
                _now(),
                tipo,
                origem,
                estado_antes,
                estado_depois,
                json.dumps(dados) if dados else None,
            ),
        )


def get_timeline(id_episodio: str) -> list:
    with get_conn() as conn:
        rows = conn.execute(
            "SELECT * FROM timeline WHERE id_episodio = ? ORDER BY timestamp",
            (id_episodio,),
        ).fetchall()
        result = []
        for r in rows:
            ev = dict(r)
            ev["dados"] = json.loads(ev["dados"]) if ev["dados"] else {}
            result.append(ev)
        return result


# ── PENDÊNCIAS ───────────────────────────────────────────────────────────────

def create_pendencia(
    id_pendencia: str,
    id_episodio: str,
    tipo: str,
    descricao: str,
    campo_afetado: Optional[str],
    bloqueia_envio: bool,
    severidade: str = "critica",
) -> dict:
    agora = _now()
    with get_conn() as conn:
        conn.execute(
            """INSERT INTO pendencias
               (id_pendencia, id_episodio, tipo, descricao, campo_afetado,
                bloqueia_envio, severidade, status, criada_em)
               VALUES (?, ?, ?, ?, ?, ?, ?, 'aberta', ?)""",
            (
                id_pendencia,
                id_episodio,
                tipo,
                descricao,
                campo_afetado,
                1 if bloqueia_envio else 0,
                severidade,
                agora,
            ),
        )
    return get_pendencia(id_pendencia)


def get_pendencia(id_pendencia: str) -> Optional[dict]:
    with get_conn() as conn:
        row = conn.execute(
            "SELECT * FROM pendencias WHERE id_pendencia = ?", (id_pendencia,)
        ).fetchone()
        return dict(row) if row else None


def resolve_pendencia(
    id_pendencia: str, resolucao: str, resolvido_por: str
) -> Optional[dict]:
    agora = _now()
    with get_conn() as conn:
        conn.execute(
            """UPDATE pendencias
               SET status = 'resolvida', resolucao = ?, resolvida_em = ?, resolvido_por = ?
               WHERE id_pendencia = ?""",
            (resolucao, agora, resolvido_por, id_pendencia),
        )
    return get_pendencia(id_pendencia)


def get_pendencias(id_episodio: str) -> list:
    with get_conn() as conn:
        rows = conn.execute(
            "SELECT * FROM pendencias WHERE id_episodio = ? ORDER BY criada_em",
            (id_episodio,),
        ).fetchall()
        return [dict(r) for r in rows]


def count_bloqueantes_abertas(id_episodio: str) -> int:
    with get_conn() as conn:
        row = conn.execute(
            """SELECT COUNT(*) AS cnt FROM pendencias
               WHERE id_episodio = ? AND bloqueia_envio = 1 AND status = 'aberta'""",
            (id_episodio,),
        ).fetchone()
        return row["cnt"]


def campos_com_pendencia_aberta(id_episodio: str) -> set:
    with get_conn() as conn:
        rows = conn.execute(
            """SELECT campo_afetado FROM pendencias
               WHERE id_episodio = ? AND status = 'aberta' AND campo_afetado IS NOT NULL""",
            (id_episodio,),
        ).fetchall()
        return {r["campo_afetado"] for r in rows}
