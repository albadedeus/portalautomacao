# -*- coding: utf-8 -*-
"""
Wrapper do pipeline CNPJ para uso no Portal de Automacoes (Flask).
Executa em background thread, reporta progresso via dict compartilhado.
Apenas um job roda por vez (lock) para respeitar o rate limit da API.
"""

import re
import threading
import uuid
import logging
from pathlib import Path
from datetime import datetime

import pipeline as _pipeline

# ─── Estado dos jobs ──────────────────────────────────────────────────────────
_jobs: dict = {}
_job_lock = threading.Lock()   # evita execucoes simultaneas


class _JobLogHandler(logging.Handler):
    """Redireciona logs do pipeline para a lista de logs do job."""

    def __init__(self, job_id: str):
        super().__init__()
        self.job_id = job_id

    def emit(self, record: logging.LogRecord) -> None:
        msg = self.format(record)
        job = _jobs.get(self.job_id)
        if job is None:
            return
        job['logs'].append(msg)

        # Extrai progresso de linhas como "[42/500] Consultando: ..."
        m = re.search(r'\[(\d+)/(\d+)\]', msg)
        if m:
            job['progresso']['atual'] = int(m.group(1))
            job['progresso']['total'] = int(m.group(2))


# ─── API publica ──────────────────────────────────────────────────────────────

def iniciar_job(sa1_bytes: bytes, sa1_filename: str, output_base: str) -> str:
    """
    Salva o arquivo SA1, cria o job e dispara o pipeline em background.
    Retorna o job_id.
    """
    job_id = uuid.uuid4().hex[:10]
    output_dir = Path(output_base) / job_id
    output_dir.mkdir(parents=True, exist_ok=True)

    sa1_path = output_dir / sa1_filename   # preserva extensão original (.csv ou .xlsx)
    sa1_path.write_bytes(sa1_bytes)

    _jobs[job_id] = {
        'status': 'aguardando',
        'logs': [f'[{datetime.now():%H:%M:%S}] Job criado. Aguardando disponibilidade...'],
        'progresso': {'atual': 0, 'total': 0},
        'arquivo_relatorio': None,
        'arquivo_clientes': None,
        'iniciado_em': datetime.now().isoformat(),
        'output_dir': str(output_dir),
        'cancelar': False,
    }

    def _run() -> None:
        job = _jobs[job_id]
        # Cancelado antes mesmo de conseguir o lock
        if job.get('cancelar'):
            job['status'] = 'cancelado'
            job['logs'].append(f'[{datetime.now():%H:%M:%S}] Cancelado antes de iniciar.')
            return

        with _job_lock:
            job = _jobs[job_id]
            if job.get('cancelar'):
                job['status'] = 'cancelado'
                job['logs'].append(f'[{datetime.now():%H:%M:%S}] Cancelado antes de iniciar.')
                return
            job['status'] = 'rodando'
            job['logs'].append(f'[{datetime.now():%H:%M:%S}] Iniciando pipeline...')

            # Backup e substituicao dos paths globais do pipeline
            _bkp = {
                'PLANILHA_SA1':    _pipeline.PLANILHA_SA1,
                'FALLBACK_SA1':    _pipeline.FALLBACK_SA1,
                'SAIDA_CLIENTES':  _pipeline.SAIDA_CLIENTES,
                'SAIDA_RELATORIO': _pipeline.SAIDA_RELATORIO,
            }
            _pipeline.PLANILHA_SA1    = sa1_path
            _pipeline.FALLBACK_SA1    = sa1_path
            _pipeline.SAIDA_CLIENTES  = output_dir / 'clientes_24meses.xlsx'
            _pipeline.SAIDA_RELATORIO = output_dir / 'relatorio_api.xlsx'
            _pipeline._stop_requested = False

            handler = _JobLogHandler(job_id)
            handler.setFormatter(
                logging.Formatter('%(asctime)s [%(levelname)s] %(message)s',
                                  datefmt='%H:%M:%S')
            )
            _pipeline.log.addHandler(handler)
            _pipeline.log.propagate = False

            try:
                _pipeline.main()
                if _pipeline._stop_requested:
                    job['status'] = 'cancelado'
                    job['logs'].append(f'[{datetime.now():%H:%M:%S}] Execução cancelada pelo usuário.')
                else:
                    job['status'] = 'concluido'
                    job['logs'].append(f'[{datetime.now():%H:%M:%S}] Pipeline concluído com sucesso.')
                job['arquivo_relatorio'] = str(output_dir / 'relatorio_api.xlsx')
                job['arquivo_clientes']  = str(output_dir / 'clientes_24meses.xlsx')
                job['logs'].append(
                    f'[{datetime.now():%H:%M:%S}] Pipeline concluido com sucesso.'
                )
            except Exception as exc:
                job['status'] = 'erro'
                job['logs'].append(f'[{datetime.now():%H:%M:%S}] ERRO FATAL: {exc}')
            finally:
                _pipeline.PLANILHA_SA1    = _bkp['PLANILHA_SA1']
                _pipeline.FALLBACK_SA1    = _bkp['FALLBACK_SA1']
                _pipeline.SAIDA_CLIENTES  = _bkp['SAIDA_CLIENTES']
                _pipeline.SAIDA_RELATORIO = _bkp['SAIDA_RELATORIO']
                _pipeline.log.removeHandler(handler)
                _pipeline.log.propagate = True

    threading.Thread(target=_run, daemon=True).start()
    return job_id


def get_job_status(job_id: str) -> dict | None:
    return _jobs.get(job_id)


def cancelar_job(job_id: str) -> bool:
    """Sinaliza o pipeline para interromper. Funciona em aguardando e rodando."""
    job = _jobs.get(job_id)
    if not job or job['status'] in ('concluido', 'cancelado', 'erro'):
        return False
    job['cancelar'] = True
    job['logs'].append(f'[{datetime.now():%H:%M:%S}] Cancelamento solicitado...')
    if job['status'] == 'rodando':
        _pipeline._stop_requested = True
    return True


def listar_jobs() -> list:
    return [
        {
            'id': jid,
            'status': j['status'],
            'iniciado_em': j.get('iniciado_em'),
            'progresso': j.get('progresso'),
        }
        for jid, j in _jobs.items()
    ]
