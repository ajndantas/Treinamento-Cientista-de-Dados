-- ITENS DA FATURA
select 
        ifat_cdfatura as fatura, 
        ifat_item as item, 
        ifat_descricao as descricao, 
        ifat_quantidade as quantidade, 
        ifat_precoinf as preco,
        ifat_tpoper as tipooperacao, 
        ifat_vlrprod as valorproduto, 
        ifat_noccusto as centrodecusto,
        ifat_vltrib as valoratribuido

from itfatura_ifat ifat -- ITENS DA FATURA
where ifat_cdfatura = '252224';