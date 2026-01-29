"""
Tratativas e lógicas avançadas de enriquecimentos de dados

Classes e funções:

apply_setor_rules(): aplica as regras de negocio na coluna tipos_de_pedido. Como usar:
df['setor'] = apply_setor_rules(df, SETOR_RULES)
"""

from dataclasses import dataclass
from typing import Iterable, Optional, Tuple
import pandas as pd
from collections.abc import Callable


# Regras de classificação de setor =======================================================

@dataclass(frozen=True)
class SetorRule:
    nome: str
    tipos_pedido: Iterable[str]
    box_range: Optional[Tuple[int, int]] = None

SETOR_RULES = [
    SetorRule(
        nome='Fracionado Pesados',
        tipos_pedido=['S01 - ENTREGA A CLIENTES'],
        box_range=(557, 584)
    ),
    
    SetorRule(
        nome='EAD - Abastecimento de Lojas',
        tipos_pedido=[
            'S13 - ABASTECIMENTO DE LOJA BOA',
            'S14 - ABASTECIMENTO DE LOJA QEB',
            'S46 - ABASTECIMENTO RETIRA LOJA',
            'S48 - ABASTECIMENTO CEL RJ',
            'S11 - TRANSF. LOJA VIA DEPOSITO BOA'
        ],
        box_range=(595, 638)
    ),

    SetorRule(
        nome='Polo - Abastecimento de Lojas',
        tipos_pedido=[
            'S13 - ABASTECIMENTO DE LOJA BOA',
            'S14 - ABASTECIMENTO DE LOJA QEB',
            'S46 - ABASTECIMENTO RETIRA LOJA',
            'S48 - ABASTECIMENTO CEL RJ',
            'S11 - TRANSF. LOJA VIA DEPOSITO BOA'
        ],
        box_range=(277, 326)
    ),

    SetorRule(
        nome='Ribeirao Preto + Uberlandia',
        tipos_pedido=['S01 - ENTGA A CLIENTES'],
        box_range=(331, 412)
    ),

    SetorRule(
        nome='Entrega Cliente + Polo-SP',
        tipos_pedido=[
            'S01 - ENTREGA A CLIENTES',
            'S02 - RETIRA CLIENTE DEPOSITO'
        ],
        box_range=(413, 556)
    ),

    SetorRule(
        nome='EAD - Balanço',
        tipos_pedido=[''
        'S53 - TRANSFERENCIA ENTRE CDS'],
        box_range=(595, 638)
    ),

    # SetorRule(
    #     nome='',
    #     tipos_pedido=[],
    #     box_range=()
    # ),

    ## Regras genéricas ##
    SetorRule(
        nome='',
        tipos_pedido=[],
    ),

    SetorRule(
        nome='Abastecimento de Lojas',
        tipos_pedido=[
            'S13 - ABASTECIMENTO DE LOJA BOA',
            'S14 - ABASTECIMENTO DE LOJA QEB',
            'S46 - ABASTECIMENTO RETIRA LOJA',
            'S48 - ABASTECIMENTO CEL RJ',
            'S11 - TRANSF. LOJA VIA DEPOSITO BOA'
        ],
    ),

    SetorRule(
        nome='Ribeirao Preto + Uberlandia',
        tipos_pedido=[
            'S01 - ENTREGA A CLIENTES',
            'S02 - RETIRA CLIENTE DEPOSITO'
        ],
    ),

    SetorRule(
        nome='Balanco',
        tipos_pedido=['S53 - TRANSFERENCIA ENTRE CDS'],
    ),

    SetorRule(
        nome='EAD',
        tipos_pedido=[
            'S05 - TRANSF EAD PROGRAMADA',
            'S04 - TRANSF EAD AUTOMATICA'
        ],
    ),

    SetorRule(
        nome='Leves',
        tipos_pedido=[
            'S39 - EXPEDICAO LEVES',
            'S39M - EXPEDICAO LEVES',
            'S39R - Single line',
            'S39R - SINGLE LINE',
            'S39P - EXPEDICAO LEVES',
            'S39I - EXPEDICAO LEVES',
        ],
    ),
]

def apply_setor_rules(
        df: pd.DataFrame,
        rules: list[SetorRule],
        default: str = 'Outras Saidas'
) -> pd.Series:
    
    box = pd.to_numeric(df['box'], errors='coerce').fillna(-1).astype(int)
    tipo = df['tipo_de_pedido']

    resultado = pd.Series(default, index=df.index, dtype='string')

    for rule in rules:
        mask = tipo.isin(rule.tipos_pedido)

        if rule.box_range:
            ini, fim = rule.box_range
            mask &= box.between(ini, fim)

        resultado[mask & (resultado == default)] = rule.nome
    
    return resultado

# Regras de SLA =======================================================

@dataclass(frozen=True)
class SLARule:
    box_range: tuple[int, int]
    deadlie_fn: Callable

SLA_RULES = {
    SLARule(
        (413, 526),
        lambda d: (d + pd.Timedelta(days=1)).dt.normalize() + pd.Timedelta(hours=5, minutes=30)
    )
}