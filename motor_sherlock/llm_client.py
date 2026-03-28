"""
motor_sherlock/llm_client.py
Cliente LLM com abstração de provedor.

Suporta: Anthropic (padrão).
Configuração via variáveis de ambiente:
  ANTHROPIC_API_KEY   — obrigatório para uso do Anthropic
  SHERLOCK_MODEL      — modelo a usar (padrão: claude-sonnet-4-6)
  SHERLOCK_MAX_TOKENS — limite de tokens por chamada (padrão: 4096)
"""

import os
import json
import logging
from typing import Any

logger = logging.getLogger(__name__)

# ── Configuração ──────────────────────────────────────────────────────────────

ANTHROPIC_API_KEY = os.environ.get("ANTHROPIC_API_KEY", "")
SHERLOCK_MODEL = os.environ.get("SHERLOCK_MODEL", "claude-sonnet-4-6")
SHERLOCK_MAX_TOKENS = int(os.environ.get("SHERLOCK_MAX_TOKENS", "4096"))


class LLMError(Exception):
    """Erro genérico de chamada ao LLM."""


def _get_anthropic_client():
    """Retorna cliente Anthropic inicializado. Levanta LLMError se chave ausente."""
    if not ANTHROPIC_API_KEY:
        raise LLMError(
            "ANTHROPIC_API_KEY não configurada. "
            "Defina a variável de ambiente antes de usar o Motor Sherlock."
        )
    try:
        import anthropic
        return anthropic.Anthropic(api_key=ANTHROPIC_API_KEY)
    except ImportError:
        raise LLMError(
            "Biblioteca 'anthropic' não instalada. "
            "Execute: pip install anthropic>=0.25.0"
        )


def call_llm(
    system_prompt: str,
    user_message: str,
    temperature: float = 0.1,
    max_tokens: int | None = None,
    expect_json: bool = False,
) -> str:
    """
    Chama o LLM com system_prompt + user_message.

    Args:
        system_prompt: Prompt de sistema (ex: Sherlock system prompt).
        user_message:  Mensagem do usuário (texto clínico ou instrução).
        temperature:   Temperatura de geração (0.1 = mais determinístico).
        max_tokens:    Limite de tokens (usa SHERLOCK_MAX_TOKENS se None).
        expect_json:   Se True, tenta parsear resposta como JSON e valida.

    Returns:
        String com a resposta do modelo.

    Raises:
        LLMError: Se a chamada falhar ou a resposta JSON for inválida quando expect_json=True.
    """
    client = _get_anthropic_client()
    tokens = max_tokens or SHERLOCK_MAX_TOKENS

    logger.info(
        "[LLM] Chamando %s (temp=%.1f, max_tokens=%d, expect_json=%s)",
        SHERLOCK_MODEL, temperature, tokens, expect_json,
    )

    try:
        response = client.messages.create(
            model=SHERLOCK_MODEL,
            max_tokens=tokens,
            temperature=temperature,
            system=system_prompt,
            messages=[{"role": "user", "content": user_message}],
        )
    except Exception as e:
        raise LLMError(f"Falha na chamada ao LLM ({SHERLOCK_MODEL}): {e}") from e

    content = response.content[0].text if response.content else ""

    if expect_json:
        try:
            # Extrai bloco JSON se vier dentro de markdown
            text = content.strip()
            if text.startswith("```"):
                lines = text.split("\n")
                inner = []
                in_block = False
                for line in lines:
                    if line.startswith("```") and not in_block:
                        in_block = True
                        continue
                    if line.startswith("```") and in_block:
                        break
                    if in_block:
                        inner.append(line)
                text = "\n".join(inner)
            json.loads(text)  # valida parse
            return text
        except (json.JSONDecodeError, ValueError) as e:
            raise LLMError(
                f"Resposta do LLM não é JSON válido: {e}\nResposta bruta: {content[:500]}"
            ) from e

    return content


def call_llm_json(
    system_prompt: str,
    user_message: str,
    temperature: float = 0.1,
) -> dict[str, Any]:
    """
    Chama o LLM e retorna resposta como dict Python (espera JSON).

    Raises:
        LLMError: Se a resposta não for JSON válido.
    """
    raw = call_llm(
        system_prompt=system_prompt,
        user_message=user_message,
        temperature=temperature,
        expect_json=True,
    )
    return json.loads(raw)
