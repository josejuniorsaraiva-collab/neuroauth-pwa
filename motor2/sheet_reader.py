"""
motor2/sheet_reader.py — BLOCO 1
Leitor real da Planilha-Mãe Motor 2.

Regras obrigatórias de leitura:
- header_row = 2 (universal para todas as abas)
- data_start = linha 3+ após dropna(how='all')
- todos os campos *_json são parseados no reader (json.loads)
- nulos são normalizados para None (não NaN)
- warnings estruturais são acumulados, não exceções

Uso:
    sheets = SheetReader(xlsx_path)
    sheets.load()
    proc_mestre = sheets.get("01_PROC_MESTRE")
"""

import os
import json
import logging
from typing import Any, Optional
from dataclasses import dataclass, field

import pandas as pd

logger = logging.getLogger(__name__)

# Caminho padrão — sobrescrito por PLANILHA_MAE_PATH em produção
# Em Render: PLANILHA_MAE_PATH=/opt/render/project/src/data/planilha_mae.xlsx
DEFAULT_XLSX_PATH = os.environ.get(
    "PLANILHA_MAE_PATH",
    os.path.join(os.path.dirname(__file__), "..", "data", "planilha_mae.xlsx"),
)

# Índice da linha de cabeçalho (0-based) — universal para todas as abas
HEADER_ROW = 2


@dataclass
class SheetData:
    """Container para dados de uma aba lida e normalizada."""
    name: str
    rows: list[dict]          # linhas normalizadas (None, não NaN; *_json parseados)
    columns: list[str]        # nomes das colunas reais
    raw_row_count: int        # contagem bruta antes de dropna
    warnings: list[str] = field(default_factory=list)

    def is_empty(self) -> bool:
        return len(self.rows) == 0

    def filter(self, **kwargs) -> list[dict]:
        """
        Filtro simples por colunas.
        filter(profile_id='ACDF_1_NIVEL', ativo=True)
        Suporta valor None como wildcard (não filtra).
        Aceita convenio_id='GLOBAL' como wildcard: retorna
        linhas onde convenio_id == valor OU convenio_id == 'GLOBAL'.
        """
        result = self.rows
        for col, val in kwargs.items():
            if val is None:
                continue
            if col == "convenio_id":
                # GLOBAL é wildcard oficial
                result = [
                    r for r in result
                    if r.get(col) == val or r.get(col) == "GLOBAL"
                ]
            elif col == "ativo":
                # Normaliza bool/string
                result = [r for r in result if _bool(r.get(col)) == val]
            else:
                result = [r for r in result if r.get(col) == val]
        return result

    def find_one(self, **kwargs) -> Optional[dict]:
        rows = self.filter(**kwargs)
        return rows[0] if rows else None


def _bool(v: Any) -> bool:
    """Normaliza booleanos do Excel (True/False/1/0/string)."""
    if isinstance(v, bool):
        return v
    if isinstance(v, (int, float)):
        return bool(v)
    if isinstance(v, str):
        return v.strip().lower() in ("true", "1", "sim", "yes", "s")
    return False


def _normalize_value(col_name: str, v: Any) -> Any:
    """
    Normaliza um valor individual:
    - NaN → None
    - campos *_json → json.loads() se string, None se nulo
    - booleanos → bool nativo
    - inteiros float (1.0) → int quando aplicável
    """
    # NaN / pd.NA
    try:
        if pd.isna(v):
            return None
    except (TypeError, ValueError):
        pass

    # Parse JSON em campos *_json
    if col_name.endswith("_json") and isinstance(v, str):
        try:
            return json.loads(v)
        except json.JSONDecodeError:
            logger.warning("JSON inválido na coluna '%s': %r", col_name, v[:100])
            return v  # retorna string bruta em vez de None para não perder dado

    # Int-like float (ex: 1.0 → 1)
    if isinstance(v, float) and v == int(v):
        return int(v)

    return v


def _read_sheet(xl: pd.ExcelFile, sheet_name: str) -> SheetData:
    """
    Lê uma aba do xlsx e retorna SheetData normalizado.
    """
    warnings: list[str] = []

    try:
        df = pd.read_excel(xl, sheet_name=sheet_name, header=HEADER_ROW)
    except Exception as e:
        warnings.append(f"Erro ao ler aba '{sheet_name}': {e}")
        return SheetData(name=sheet_name, rows=[], columns=[], raw_row_count=0, warnings=warnings)

    # Remove colunas totalmente sem nome (Unnamed: X)
    real_cols = [c for c in df.columns if not str(c).startswith("Unnamed")]
    df = df[real_cols]

    # Remove linha de descrição (primeira linha após header, geralmente a nota da seção)
    # Heurística: primeira linha onde a primeira coluna contém descrição da tabela (str longa)
    # Solução mais robusta: descartar linhas onde a primeira coluna nomeada não parece um ID
    if len(df) > 0:
        first_col = real_cols[0] if real_cols else None
        if first_col:
            first_val = df.iloc[0][first_col]
            # Se o primeiro valor é uma string longa (>40 chars) → linha de descrição
            if isinstance(first_val, str) and len(first_val) > 40:
                df = df.iloc[1:].reset_index(drop=True)

    raw_count = len(df)

    # Remove linhas completamente nulas
    df = df.dropna(how="all").reset_index(drop=True)

    # Normaliza cada célula
    rows = []
    for _, row in df.iterrows():
        normalized = {}
        for col in real_cols:
            normalized[col] = _normalize_value(col, row.get(col))
        rows.append(normalized)

    return SheetData(
        name=sheet_name,
        rows=rows,
        columns=real_cols,
        raw_row_count=raw_count,
        warnings=warnings,
    )


class SheetReader:
    """
    Leitor principal da Planilha-Mãe.

    Carrega todas as abas em memória na inicialização.
    Expõe acesso tipado por nome de aba.
    Acumula warnings estruturais (gaps, abas vazias, campos ausentes).
    """

    # Abas obrigatórias para o motor funcionar
    REQUIRED_SHEETS = [
        "01_PROC_MESTRE",
        "02_PROC_ALIAS",
        "05_MAPEAMENTO_CODIGOS",
        "07_CIDS_PERMITIDOS",
        "09_REGRAS_DECISAO",
        "12_MODELOS_DOCUM",
        "15_OPME_REGRAS",
        "16_OPME_CATALOGO",
        "20_PESOS",
    ]

    # Pesos default quando convênio não tem linha em 20_PESOS
    DEFAULT_PESOS = {
        "peso_regulatorio": 0.25,
        "peso_convenio":    0.25,
        "peso_historico":   0.20,
        "peso_documental":  0.20,
        "peso_opme":        0.10,
    }

    def __init__(self, xlsx_path: Optional[str] = None):
        self.xlsx_path = xlsx_path or DEFAULT_XLSX_PATH
        self._sheets: dict[str, SheetData] = {}
        self.structural_warnings: list[str] = []
        self._loaded = False

    def load(self) -> "SheetReader":
        """Carrega todas as abas. Pode ser chamado múltiplas vezes (reload)."""
        if not os.path.exists(self.xlsx_path):
            raise FileNotFoundError(
                f"Planilha-Mãe não encontrada em: {self.xlsx_path}\n"
                f"Defina PLANILHA_MAE_PATH com o caminho correto."
            )

        xl = pd.ExcelFile(self.xlsx_path)
        real_names = xl.sheet_names

        for name in real_names:
            self._sheets[name] = _read_sheet(xl, name)

        self._run_structural_checks()
        self._loaded = True
        logger.info(
            "SheetReader carregado: %d abas, %d warnings estruturais",
            len(self._sheets),
            len(self.structural_warnings),
        )
        return self

    def get(self, sheet_name: str) -> SheetData:
        """Retorna SheetData de uma aba. Lança KeyError se não existir."""
        if not self._loaded:
            raise RuntimeError("Chame SheetReader.load() antes de get()")
        if sheet_name not in self._sheets:
            raise KeyError(f"Aba '{sheet_name}' não encontrada na planilha. "
                           f"Disponíveis: {list(self._sheets.keys())}")
        return self._sheets[sheet_name]

    def sheet_names(self) -> list[str]:
        return list(self._sheets.keys())

    def _run_structural_checks(self):
        """Verifica gaps conhecidos e acumula warnings estruturais."""
        w = self.structural_warnings

        # Abas obrigatórias ausentes
        for required in self.REQUIRED_SHEETS:
            if required not in self._sheets:
                w.append(f"CRÍTICO: Aba obrigatória '{required}' ausente na planilha")

        # ACDF_1_NIVEL ausente em PROC_MESTRE
        if "01_PROC_MESTRE" in self._sheets:
            proc = self._sheets["01_PROC_MESTRE"]
            profile_ids = {r["profile_id"] for r in proc.rows if r.get("profile_id")}
            alias_sheet = self._sheets.get("02_PROC_ALIAS")
            if alias_sheet:
                alias_profiles = {r["profile_id"] for r in alias_sheet.rows if r.get("profile_id")}
                orphan_profiles = alias_profiles - profile_ids
                for orphan in sorted(orphan_profiles):
                    w.append(
                        f"profile_master_missing: profile_id '{orphan}' referenciado em "
                        f"02_PROC_ALIAS mas ausente em 01_PROC_MESTRE — "
                        f"motor operará em modo degradado controlado para este perfil"
                    )

        # METRICAS vazia
        if "19_METRICAS" in self._sheets and self._sheets["19_METRICAS"].is_empty():
            w.append(
                "metricas_vazias: 19_METRICAS sem dados históricos — "
                "score_historico será 0.5 (neutro) para todos os perfis"
            )

        # PESOS incompleto
        if "20_PESOS" in self._sheets:
            pesos = self._sheets["20_PESOS"]
            convenios_com_peso = {r.get("convenio_id") for r in pesos.rows}
            w.append(
                f"pesos_parciais: 20_PESOS contém {len(pesos.rows)} linha(s) "
                f"({', '.join(str(c) for c in convenios_com_peso)}) — "
                f"convênios sem linha usarão pesos default {self.DEFAULT_PESOS}"
            )

        # Abas de schema-only (informativo)
        for empty_sheet in ["10_REGRAS_ALERTA", "11_REGRAS_BLOQUEIO", "18_HIST_DESFECHOS", "21_DECISION_RUNS"]:
            if empty_sheet in self._sheets and self._sheets[empty_sheet].is_empty():
                w.append(f"schema_only: '{empty_sheet}' sem dados — será alvo de escrita futura")

        for sw in w:
            logger.warning("[STRUCTURAL] %s", sw)


# ── Singleton carregado na inicialização do módulo ────────────────────────────

_reader: Optional[SheetReader] = None


def get_reader(xlsx_path: Optional[str] = None) -> SheetReader:
    """
    Retorna o SheetReader singleton.
    Primeira chamada carrega o xlsx.
    """
    global _reader
    if _reader is None or xlsx_path:
        _reader = SheetReader(xlsx_path).load()
    return _reader


def reload_reader(xlsx_path: Optional[str] = None) -> SheetReader:
    """Força reload — útil após atualização do xlsx em runtime."""
    global _reader
    _reader = SheetReader(xlsx_path or (DEFAULT_XLSX_PATH if _reader is None else _reader.xlsx_path)).load()
    return _reader
