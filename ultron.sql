

-- ultron_respostas_vazio
select 1 as pergunta_id,
     collect_list(
             named_struct(
                    "respostaId", 0,
                    "descricaoResposta", "",
                    "ordemResposta", 0,
                    "proximaPerguntaId", 0,
                    "razaoResposta", 0  )
     ) as Respostas

--ultron_perguntas_vazio
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
        from ultron_respostas_vazio rmob

--ultron_itens_nao_inc_vazio
select 0 as servico_id,
     1 as plano_assistencia_id,
     collect_list(
            named_struct(
                "descricaoItem", ""
                )
         ) as Descricao


--tb_teste_mob0 tipoFranquiaServico fixo M
select ofp.id_oferta_plano,
             collect_list(
                    named_struct(
                        "servicoId", concat("1000", ass.id_assistencia),
                        "servicoCodigoMapfre", "",
                        "nomeServico", nvl(ass.nm_assistencia, ""),
                        "descricaoServico", nvl(ass.tx_descricao, ""),
                        "franquiaValorMonetario", nvl(ass.vl_assistencia, 0),
                        "franquiaQuantidade", nvl(pass.qt_limite_acionamento, 0),
                        "limiteAcionamentos", nvl(pass.qt_limite_acionamento, 0),
                        "tipoFranquiaServico",  "M",
                        "prioritario",false,
                        "ordem", 100,
                        "diasAcionamento", 0,
                        "itensNaoInclusos", itens.Descricao.descricaoItem,
                        "questionario", perg.Perguntas)
                ) Servicos
                from oferta_plano ofp,
                ultron_itens_nao_inc_vazio as itens,
                ultron_perguntas_vazio as perg
                left join oferta_plano_pacote_assistencia ofpa on ofpa.id_oferta_plano = ofp.id_oferta_plano and ofpa.linha = 1
                left join oferta_pacote_assistencia opa on opa.id_oferta_pacote_assistencia = ofpa.id_oferta_pacote_assistencia and opa.linha = 1
                left join chassi_pacote_assistencia cpa on cpa.id_chassi_pacote_assistencia = opa.id_chassi_pacote_assistencia and cpa.linha = 1
                left join pacote_assistencia pass on pass.id_pacote_assistencia = cpa.id_pacote_assistencia and pass.linha = 1
                left join assistencia_pacote_assistencia apass on apass.id_pacote_assistencia = pass.id_pacote_assistencia and apass.linha = 1
                left join assistencia ass on ass.id_assistencia = apass.id_assistencia and ass.linha = 1
                where ofp.linha = 1
                group by ofp.id_oferta_plano


--tb_teste_mob1 -- definir plano top referencia
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

--tb_teste_mob2 final
select concat("1000", pl.id_oferta_plano)  as planoId,
     nvl(pl.nm_plano, "") as nome,
     cast("0" as LONG) as numeroContrato,
     case when ptop.id_oferta_plano is not null then true else false end planoReferencia,
     smob.Servicos as servicos,
    null categorias
    from oferta_plano as pl
     left join tb_teste_mob1 as ptop on ptop.id_oferta_plano = pl.id_oferta_plano
     left join tb_teste_mob0 as smob on smob.id_oferta_plano = pl.id_oferta_plano
     where pl.linha = 1
