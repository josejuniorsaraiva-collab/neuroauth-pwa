"""
motor_sherlock — Camada de raciocínio clínico por LLM sobre Motor 2.

Arquitetura Option C (híbrida):
  1. LLM (claude-sonnet + Sherlock system prompt) extrai parâmetros estruturados
     a partir de texto clínico livre.
  2. Motor 2 valida tudo contra a planilha-mãe (fonte de verdade determinística).
  3. LLM sintetiza o artefato do Motor 2 em narrativa clínica legível.

Ponto de entrada:
    from motor_sherlock.sherlock_engine import run_sherlock
"""
