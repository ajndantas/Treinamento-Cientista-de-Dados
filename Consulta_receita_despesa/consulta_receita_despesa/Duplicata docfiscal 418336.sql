select *
from v_receita_sng_estado
where documentofiscal = '418336';

-- O PROBLEMA ESTÁ AQUI
select * -- 2 Ocorrências, 2 documentos de pagamento
from tbl_tmp_receita_sng
where titulo = '252516';

select *
from tbl_tmp_receita_sng;

               
select * -- CORRETO
from TBL_COB_FENASEG
where cod_cliente = (
                        select distinct cod_cadu -- 2 Ocorrências, 2 documentos de pagamento sem o distinct
                        from tbl_tmp_receita_sng
                        where titulo = (
                                        select distinct titulo
                                        from v_receita_sng_estado
                                        where documentofiscal = '418336'
                                       )
                    )
and competencia = (
                    select distinct competencia -- 2 Ocorrências, 2 documentos de pagamento
                    from tbl_tmp_receita_sng
                    where titulo = (
                                    select distinct titulo
                                    from v_receita_sng_estado
                                    where documentofiscal = '418336'
                                   )
                  );