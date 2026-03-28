"""
motor2/batch_runner.py — Runner de casos em lote

Propósito:
  Executar Motor 2 em lote sobre uma lista de casos JSON reais,
  gerar relatório de resultados e alimentar 21_DECISION_RUNS continuamente.

Uso:
  # Linha de comando
  python -m motor2.batch_runner --casos casos_reais.json [--xlsx planilha.xlsx]

  # Programático
  from motor2.batch_runner import run_batch
  relatorio = run_batch(casos, xlsx_path="...")

Formato de entrada (lista de dicts):
  [
    {
      "episodio_id":     "EP_001",          # obrigatório
      "profile_texto":   "acdf 1 nivel",    # obrigatório
      "convenio_id":     "UNIMED_CARIRI",   # obrigatório
      "cid_principal":   "M47.1",           # obrigatório
      "carater":         "eletivo",         # default: eletivo
      "niveis":          1,                 # default: 1
      "hospital_id":     null,              # opcional
      "cbo_executor":    "225118",          # opcional
      "opme_context":    null,              # opcional
      "clinical_context": {                 # opcional
        "indicacao_clinica": "...",
        "tto_conservador_semanas": 8
      }
    },
    ...
  ]

Formato de saída (BatchRelatorio):
  {
    "total": 30,
    "go": 12,
    "go_com_ressalvas": 14,
    "no_go": 4,
    "erros": 0,
    "taxa_go": 0.40,
    "taxa_no_go": 0.13,
    "score_medio": 0.824,
    "timestamp": "2026-03-28T...",
    "casos": [DecisionArtefact.to_dict(), ...]
  }
"""

import json
import logging
import argparse
import sys
import os
from dataclasses import dataclass, field, asdict
from datetime import datetime, timezone
from typing import Optional

logger = logging.getLogger(__name__)


@dataclass
class BatchRelatorio:
    total: int
    go: int
    go_com_ressalvas: int
    no_go: int
    erros: int
    taxa_go: float
    taxa_no_go: float
    score_medio: float
    timestamp: str
    casos: list[dict] = field(default_factory=list)
    erros_detalhes: list[dict] = field(default_factory=list)

    def to_dict(self) -> dict:
        return asdict(self)

    def print_summary(self):
        sep = "=" * 60
        print(f"\n{sep}")
        print(f"BATCH NEUROAUTH Motor 2 — {self.timestamp}")
        print(sep)
        print(f"  Total de casos   : {self.total}")
        print(f"  GO               : {self.go} ({self.taxa_go:.0%})")
        print(f"  GO_COM_RESSALVAS : {self.go_com_ressalvas} ({self.go_com_ressalvas/self.total:.0%})")
        print(f"  NO_GO            : {self.no_go} ({self.taxa_no_go:.0%})")
        print(f"  Erros runtime    : {self.erros}")
        print(f"  Score médio      : {self.score_medio:.4f}")
        print(sep)
        for caso in self.casos:
            status = caso.get("status_decisao", "?")
            score = caso.get("score_resumo", {}).get("score_final", 0)
            profile = caso.get("profile_id", "?")
            ep = caso.get("episodio_id", "?")
            alerts = len(caso.get("alertas", []))
            blocks = len(caso.get("bloqueios", []))
            flag = "✅" if status == "GO" else ("⚠️ " if status == "GO_COM_RESSALVAS" else "🚫")
            print(f"  {flag} {ep:20s} | {profile:30s} | score={score:.3f} | A={alerts} B={blocks}")
        if self.erros_detalhes:
            print(f"\n  ERROS ({self.erros}):")
            for e in self.erros_detalhes:
                print(f"    [{e['episodio_id']}] {e['erro']}")
        print(sep)


def run_batch(
    casos: list[dict],
    xlsx_path: Optional[str] = None,
    salvar_json: Optional[str] = None,
) -> BatchRelatorio:
    """
    Executa Motor 2 para cada caso da lista.

    Tolerante a falhas: caso individual que falhar vai para erros_detalhes,
    não interrompe o lote.

    Args:
      casos:       lista de dicts no formato de entrada
      xlsx_path:   caminho do xlsx (usa PLANILHA_MAE_PATH se None)
      salvar_json: se informado, salva relatório completo neste path .json

    Returns:
      BatchRelatorio com estatísticas e lista de DecisionArtefact.to_dict()
    """
    # Import aqui para evitar circular import
    from motor2.output_builder import run_motor2
    import motor2.sheet_reader as sr
    sr._reader = None  # força reload com o xlsx informado

    timestamp = datetime.now(timezone.utc).isoformat()
    results: list[dict] = []
    erros: list[dict] = []

    logger.info("Batch iniciado: %d casos | xlsx=%s", len(casos), xlsx_path or "env")

    for i, caso in enumerate(casos):
        ep_id = caso.get("episodio_id", f"EP_BATCH_{i:04d}")
        try:
            artefact = run_motor2(
                episodio_id=ep_id,
                profile_texto=caso.get("profile_texto", ""),
                convenio_id=caso.get("convenio_id", ""),
                hospital_id=caso.get("hospital_id"),
                carater=caso.get("carater", "eletivo"),
                niveis=int(caso.get("niveis", 1)),
                cid_principal=caso.get("cid_principal", ""),
                cid_secundarios=caso.get("cid_secundarios", []),
                cbo_executor=caso.get("cbo_executor"),
                opme_context=caso.get("opme_context"),
                clinical_context=caso.get("clinical_context"),
                xlsx_path=xlsx_path,
            )
            results.append(artefact.to_dict())
            logger.info(
                "  [%d/%d] %s → %s (score=%.3f)",
                i + 1, len(casos), ep_id,
                artefact.status_decisao,
                artefact.score_resumo.get("score_final", 0),
            )
        except Exception as e:
            erros.append({"episodio_id": ep_id, "erro": str(e)})
            logger.error("  [%d/%d] %s → ERRO: %s", i + 1, len(casos), ep_id, e)

    # Estatísticas
    n_go = sum(1 for r in results if r.get("status_decisao") == "GO")
    n_ressalvas = sum(1 for r in results if r.get("status_decisao") == "GO_COM_RESSALVAS")
    n_nogo = sum(1 for r in results if r.get("status_decisao") == "NO_GO")
    scores = [r.get("score_resumo", {}).get("score_final", 0) for r in results]
    score_medio = round(sum(scores) / len(scores), 4) if scores else 0.0
    total = len(casos)

    relatorio = BatchRelatorio(
        total=total,
        go=n_go,
        go_com_ressalvas=n_ressalvas,
        no_go=n_nogo,
        erros=len(erros),
        taxa_go=round(n_go / total, 4) if total else 0.0,
        taxa_no_go=round(n_nogo / total, 4) if total else 0.0,
        score_medio=score_medio,
        timestamp=timestamp,
        casos=results,
        erros_detalhes=erros,
    )

    if salvar_json:
        with open(salvar_json, "w", encoding="utf-8") as f:
            json.dump(relatorio.to_dict(), f, ensure_ascii=False, indent=2, default=str)
        logger.info("Relatório salvo: %s", salvar_json)

    return relatorio


def _build_template_casos() -> list[dict]:
    """Retorna 20 casos sintéticos cobrindo todos os profiles e convênios."""
    profiles = [
        ("artrodese cervical anterior 1 nivel",  "ACDF_1_NIVEL",   "M47.1"),
        ("artrodese cervical anterior 2 niveis",  "ACDF_2_NIVEIS",  "M47.1"),
        ("artroplastia cervical",                 "ARTRO",          "M50.1"),
        ("microdiscectomia lombar",               "MICRO",          "M51.1"),
        ("plif lombar",                           "PLIF",           "M51.1"),
        ("tlif lombar",                           "TLIF",           "M51.1"),
    ]
    convenios = ["UNIMED_CARIRI", "SULAMERICA", "HAPVIDA", "BRADESCO_SAUDE"]
    casos = []
    idx = 1
    for (texto, pid, cid) in profiles:
        for conv in convenios[:2]:  # 2 convênios por profile = 12 combinações
            tto_semanas = 8 if conv != "SULAMERICA" else (4 if idx % 3 == 0 else 8)
            casos.append({
                "episodio_id": f"EP_BATCH_{idx:04d}",
                "profile_texto": texto,
                "convenio_id": conv,
                "carater": "eletivo",
                "niveis": 2 if "2 niveis" in texto else 1,
                "cid_principal": cid,
                "cbo_executor": "225118",
                "opme_context": {"numero_marcas": 3, "itens": []} if "artrod" in texto.lower() else None,
                "clinical_context": {
                    "indicacao_clinica": f"Caso sintético {idx} — {texto} com indicação clínica detalhada para validação",
                    "tto_conservador_semanas": tto_semanas,
                    "falha_conservador": tto_semanas >= 6,
                },
            })
            idx += 1
    # Adicionar 8 casos extremos
    extremos = [
        {"episodio_id": "EP_BATCH_0021", "profile_texto": "trombectomia mecanica cerebral", "convenio_id": "UNIMED_CARIRI", "carater": "urgencia", "niveis": 1, "cid_principal": "I63.0", "cbo_executor": "225118", "opme_context": {"numero_marcas": 3, "itens": []}, "clinical_context": {"indicacao_clinica": "AVC isquêmico agudo OAB — trombectomia mecânica emergência"}},
        {"episodio_id": "EP_BATCH_0022", "profile_texto": "clipagem aneurisma", "convenio_id": "UNIMED_CARIRI", "carater": "urgencia", "niveis": 1, "cid_principal": "I67.1", "cbo_executor": "225120", "opme_context": {"numero_marcas": 3, "itens": [{"item": "clipe_yasargil", "registro_anvisa": "10394010001"}]}, "clinical_context": {"indicacao_clinica": "Aneurisma intracraniano roto com HSA grau III Fisher"}},
        {"episodio_id": "EP_BATCH_0023", "profile_texto": "artrodese cervical anterior", "convenio_id": "SULAMERICA", "carater": "eletivo", "niveis": 1, "cid_principal": "M54.2", "cbo_executor": "225118", "opme_context": {"numero_marcas": 1, "itens": []}, "clinical_context": {"indicacao_clinica": "Dor", "tto_conservador_semanas": 2}},  # deveria NO_GO: SULAMERICA sem tto + OPME marcas insuficientes
        {"episodio_id": "EP_BATCH_0024", "profile_texto": "microdiscectomia lombar", "convenio_id": "SULAMERICA", "carater": "eletivo", "niveis": 1, "cid_principal": "M51.1", "cbo_executor": "225118", "opme_context": None, "clinical_context": {"indicacao_clinica": "Hérnia L4-L5 com radiculopatia S1 refratária 8 semanas fisioterapia e AINE", "tto_conservador_semanas": 8}},  # deve GO
        {"episodio_id": "EP_BATCH_0025", "profile_texto": "dvp hidrocefalia", "convenio_id": "HAPVIDA", "carater": "urgencia", "niveis": 1, "cid_principal": "G91.1", "cbo_executor": None, "opme_context": None, "clinical_context": {"indicacao_clinica": "Hidrocefalia obstrutiva com hipertensão intracraniana"}},
    ]
    casos.extend(extremos)
    return casos


# ── CLI ────────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")

    parser = argparse.ArgumentParser(description="NEUROAUTH Motor 2 — Batch Runner")
    parser.add_argument("--casos", help="Path para JSON de casos (lista). Se omitido, usa casos sintéticos.")
    parser.add_argument("--xlsx", help="Path do xlsx da Planilha-Mãe")
    parser.add_argument("--out", default="/tmp/batch_relatorio.json", help="Onde salvar o relatório JSON")
    args = parser.parse_args()

    if args.casos:
        with open(args.casos, encoding="utf-8") as f:
            casos = json.load(f)
    else:
        print("Nenhum arquivo de casos informado — usando suite sintética de 25 casos.")
        casos = _build_template_casos()

    relatorio = run_batch(casos, xlsx_path=args.xlsx, salvar_json=args.out)
    relatorio.print_summary()
    print(f"\nRelatório completo: {args.out}")
