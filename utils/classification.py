"""
Tratativas e lógicas avançadas de enriquecimentos de dados

Classes e funções:
"""

from dataclasses import dataclass
from typing import Iterable, Optional, Tuple
import pandas as pd

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
        tipos_pedido=[],
    ),
]