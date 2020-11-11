

{

  "0_respostas_stage": "select 1 as pergunta_id, collect_list( named_struct( \"respostaId\", 0, \"descricaoResposta\", \"\", \"ordemResposta\", 0, \"proximaPerguntaId\", 0, \"razaoResposta\", 0 ) ) as Respostas\n" ,
  "1_perguntas_stage": "select 1 as questionario_id, collect_list( named_struct( \"perguntaId\", 0, \"descricaoPergunta\", \"\", \"perguntaPreparacao\", false, \"multiplasRespostas\", false, \"razaoPergunta\", \"\", \"respostas\", rmob.Respostas) ) as Perguntas from 0_respostas_stage rmob where 3 > 5\n",
  "2_itens_stage": "select 0 as servico_id, 1 as plano_assistencia_id, collect_list( named_struct( \"descricaoItem\", \"\" ) ) as Descricao where 3 > 5\n",
  "3_servicos_stage": "select ofp.id_oferta_plano, collect_list( named_struct( \"servicoId\", case when ass.id_assistencia is null then 0 else cast(100000 + nvl(ass.id_assistencia, 0) as int) end , \"servicoCodigoMapfre\", \"1000\", \"nomeServico\", nvl(ass.nm_assistencia, \"\"), \"descricaoServico\", nvl(ass.tx_descricao, \"\"), \"franquiaValorMonetario\", cast(nvl(ass.vl_assistencia, 0) as double), \"franquiaQuantidade\", nvl(pass.qt_limite_acionamento, 0), \"limiteAcionamentos\", nvl(pass.qt_limite_acionamento, 0), \"tipoFranquiaServico\", \"M\", \"prioritario\",false, \"ordem\", 100, \"diasAcionamento\", 0, \"itensNaoInclusos\", itens.Descricao.descricaoItem, \"questionario\", perg.Perguntas) ) Servicos from oferta_plano ofp cross join 2_itens_stage as itens cross join 1_perguntas_stage as perg left join oferta_plano_pacote_assistencia ofpa on ofpa.id_oferta_plano = ofp.id_oferta_plano and ofpa.linha = 1 left join oferta_pacote_assistencia opa on opa.id_oferta_pacote_assistencia = ofpa.id_oferta_pacote_assistencia and opa.linha = 1 left join chassi_pacote_assistencia cpa on cpa.id_chassi_pacote_assistencia = opa.id_chassi_pacote_assistencia and cpa.linha = 1 left join pacote_assistencia pass on pass.id_pacote_assistencia = cpa.id_pacote_assistencia and pass.linha = 1 left join assistencia_pacote_assistencia apass on apass.id_pacote_assistencia = pass.id_pacote_assistencia and apass.linha = 1 left join assistencia ass on ass.id_assistencia = apass.id_assistencia and ass.linha = 1 where ofp.linha = 1 group by ofp.id_oferta_plano\n",
  "4_plano_top_stage": "select principal.id_oferta_plano from ( select ofp.id_oferta_plano, count(pass.id_pacote_assistencia) as qtdPacotes from oferta_plano ofp join oferta_plano_pacote_assistencia ofpa on ofpa.id_oferta_plano = ofp.id_oferta_plano and ofpa.linha = 1 join oferta_pacote_assistencia opa on opa.id_oferta_pacote_assistencia = ofpa.id_oferta_pacote_assistencia and opa.linha = 1 join chassi_pacote_assistencia cpa on cpa.id_chassi_pacote_assistencia = opa.id_chassi_pacote_assistencia and cpa.linha = 1 join pacote_assistencia pass on pass.id_pacote_assistencia = cpa.id_pacote_assistencia and pass.linha = 1 where ofp.linha = 1 group by ofp.id_oferta_plano ) principal where principal.qtdPacotes = ( select max(a.qtdPacotes) from ( select ofp.id_oferta_plano, count(pass.id_pacote_assistencia) as qtdPacotes from oferta_plano ofp join oferta_plano_pacote_assistencia ofpa on ofpa.id_oferta_plano = ofp.id_oferta_plano and ofpa.linha = 1 join oferta_pacote_assistencia opa on opa.id_oferta_pacote_assistencia = ofpa.id_oferta_pacote_assistencia and opa.linha = 1 join chassi_pacote_assistencia cpa on cpa.id_chassi_pacote_assistencia = opa.id_chassi_pacote_assistencia and cpa.linha = 1 join pacote_assistencia pass on pass.id_pacote_assistencia = cpa.id_pacote_assistencia and pass.linha = 1 where ofp.linha = 1 group by ofp.id_oferta_plano ) a )\n",
  "5_planos_final_stage": "select cast(100000 + nvl(pl.id_oferta_plano, 0) as int) as planoId, nvl(pl.nm_plano, '') as nome, case when lower(nvl(pl.nm_plano, '')) like '%essencial%' then 548441 when lower(nvl(pl.nm_plano, '')) like '%total%' then 551137 else 551139 end as numeroContrato, case when ptop.id_oferta_plano is not null then true else false end planoReferencia, smob.Servicos as servicos, null categorias from oferta_plano as pl left join 4_plano_top_stage as ptop on ptop.id_oferta_plano = pl.id_oferta_plano left join 3_servicos_stage as smob on smob.id_oferta_plano = pl.id_oferta_plano where pl.linha = 1\n"
}
-- 0_respostas_stage respostas
select 1 as pergunta_id,
     collect_list(
             named_struct(
                    "respostaId", 0,
                    "descricaoResposta", "",
                    "ordemResposta", 0,
                    "proximaPerguntaId", 0,
                    "razaoResposta", 0  )
     ) as Respostas

--1_perguntas_stage perguntas
select 1 as questionario_id,
        collect_list(
                    named_struct(
                        "perguntaId", 0,
                        "descricaoPergunta", "",
                        "perguntaPreparacao", false,
                        "multiplasRespostas", false,
                        "razaoPergunta", "",
                        "respostas", rmob.Respostas)
            ) as Perguntas
        from 0_respostas_stage rmob
        where 3 > 5

--2_itens_stage itens
select nvl(ord.id_servico, 0) as id_assistencia,
       nvl(ord.id_ordem, 100) as ordem,
       case  when lower(nvl(ord.emergencial, '')) = 's' then true else false end prioritario,
     collect_list(
            named_struct(
                'descricaoItem', nvl(ord.itens_nao_inclusos, '')
                )
         ) as Descricao
      from ultron_assistencia_ordenacao ord
      group by id_assistencia, ordem, prioritario


--3_servicos_stage
select ofp.id_oferta_plano,
             collect_list(
                    named_struct(
                        'servicoId', case  when ass.id_assistencia is null then 0 else cast(100000 + nvl(ass.id_assistencia, 0) as int)  end ,
                        'servicoCodigoMapfre', '1000',
                        'nomeServico', nvl(ass.nm_assistencia, ''),
                        'descricaoServico', nvl(ass.tx_descricao, ''),
                        'franquiaValorMonetario', cast(nvl(ass.vl_assistencia, 0) as double),
                        'franquiaQuantidade', nvl(pass.qt_limite_acionamento, 0),
                        'limiteAcionamentos', nvl(pass.qt_limite_acionamento, 0),
                        'tipoFranquiaServico',  'M',
                        'prioritario',nvl(itens.prioritario, false),
                        'ordem', nvl(itens.ordem, 100),
                        'diasAcionamento', 0,
                        'itensNaoInclusos', itens.Descricao.descricaoItem,
                        'questionario', perg.Perguntas)
                ) Servicos
                from oferta_plano ofp
                cross join 1_perguntas_stage as perg
                left join oferta_plano_pacote_assistencia ofpa on ofpa.id_oferta_plano = ofp.id_oferta_plano and ofpa.linha = 1
                left join oferta_pacote_assistencia opa on opa.id_oferta_pacote_assistencia = ofpa.id_oferta_pacote_assistencia and opa.linha = 1
                left join chassi_pacote_assistencia cpa on cpa.id_chassi_pacote_assistencia = opa.id_chassi_pacote_assistencia and cpa.linha = 1
                left join pacote_assistencia pass on pass.id_pacote_assistencia = cpa.id_pacote_assistencia and pass.linha = 1
                left join assistencia_pacote_assistencia apass on apass.id_pacote_assistencia = pass.id_pacote_assistencia and apass.linha = 1
                left join assistencia ass on ass.id_assistencia = apass.id_assistencia and ass.linha = 1
                left join 2_itens_stage as itens on itens.id_assistencia = ass.id_assistencia
                where ofp.linha = 1
                group by ofp.id_oferta_plano




--4_plano_top_stage
select principal.id_oferta_plano from
    (
        select
            ofp.id_oferta_plano,  count(pass.id_pacote_assistencia) as qtdPacotes
            from oferta_plano ofp
        join oferta_plano_pacote_assistencia ofpa on ofpa.id_oferta_plano = ofp.id_oferta_plano and ofpa.linha = 1
        join oferta_pacote_assistencia opa on opa.id_oferta_pacote_assistencia = ofpa.id_oferta_pacote_assistencia and opa.linha = 1
        join chassi_pacote_assistencia cpa on cpa.id_chassi_pacote_assistencia = opa.id_chassi_pacote_assistencia and cpa.linha = 1
        join pacote_assistencia pass on pass.id_pacote_assistencia = cpa.id_pacote_assistencia and pass.linha = 1
        where ofp.linha = 1
        group by  ofp.id_oferta_plano
    ) principal

    where principal.qtdPacotes = (
            select  max(a.qtdPacotes)
            from (
            select
                ofp.id_oferta_plano,  count(pass.id_pacote_assistencia) as qtdPacotes
                from oferta_plano ofp
            join oferta_plano_pacote_assistencia ofpa on ofpa.id_oferta_plano = ofp.id_oferta_plano and ofpa.linha = 1
            join oferta_pacote_assistencia opa on opa.id_oferta_pacote_assistencia = ofpa.id_oferta_pacote_assistencia and opa.linha = 1
            join chassi_pacote_assistencia cpa on cpa.id_chassi_pacote_assistencia = opa.id_chassi_pacote_assistencia and cpa.linha = 1
            join pacote_assistencia pass on pass.id_pacote_assistencia = cpa.id_pacote_assistencia and pass.linha = 1
            where ofp.linha = 1
            group by  ofp.id_oferta_plano
                ) a
    )

--5_planos_final_stage final
select cast(100000 + nvl(pl.id_oferta_plano, 0) as int)  as planoId,
     nvl(pl.nm_plano, '') as nome,
     case  when lower(nvl(pl.nm_plano, '')) like '%essencial%' then 548441  when lower(nvl(pl.nm_plano, '')) like '%total%' then 551137  else 551139 end as numeroContrato,
     case when ptop.id_oferta_plano is not null then true else false end planoReferencia,
     smob.Servicos as servicos,
    null categorias
    from oferta_plano as pl
     left join 4_plano_top_stage as ptop on ptop.id_oferta_plano = pl.id_oferta_plano
     left join 3_servicos_stage as smob on smob.id_oferta_plano = pl.id_oferta_plano
     where pl.linha = 1

