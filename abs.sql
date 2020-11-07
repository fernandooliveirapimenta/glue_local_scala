

--tb_teste_mob0
select rmob.pergunta_id,
     collect_list(
             named_struct(
                    "respostaId", rmob.resposta_id,
                    "descricaoResposta", nvl(rmob.resposta, ""),
                    "ordemResposta", nvl(rmob.ordem, 0),
                    "proximaPerguntaId", rmob.proxima_pergunta,
                    "razaoResposta", nvl(rmob.razao, "")  )
     ) as Respostas
     from assistencia_questionario_resposta_tb rmob
     where rmob.linha = 1
      group by rmob.pergunta_id;

--tb_teste_mob1
select pmob.questionario_id,
        collect_list(
                    named_struct(
                        "perguntaId", pmob.pergunta_id,
                        "descricaoPergunta", nvl(pmob.pergunta, ""),
                        "perguntaPreparacao", case  when pmob.preparacao = "S" then true  when pmob.preparacao = "N" then false  else false end,
                        "multiplasRespostas", case  when pmob.multiplas_respostas = "S" then true  when pmob.multiplas_respostas = "N" then false  else false end,
                        "razaoPergunta", pmob.razao,
                        "respostas", rmob.Respostas)
            ) as Perguntas
        from assistencia_questionario_pergunta_tb pmob
         left join tb_teste_mob0 rmob  on pmob.pergunta_id = rmob.pergunta_id
          where pmob.linha = 1 group by pmob.questionario_id;

--tb_teste_mob2
select itn.servico_id,
     itn.plano_assistencia_id,
     collect_list(
            named_struct(
                "descricaoItem", nvl(itn.descricao, "")
                )
         ) as Descricao
     from itens_nao_inclusos_tb as itn
      where itn.linha = 1
      group by itn.servico_id, itn.plano_assistencia_id;

--tb_teste_mob3
select ass.plano_assistencia_id,
             collect_list(
                    named_struct(
                        "servicoId", serv.servico_id,
                        "servicoCodigoMapfre", nvl(serv.codigo_ama, ""),
                        "nomeServico", nvl(serv.txt_servico, ""),
                        "descricaoServico", nvl(serv.descricao, ""),
                        "franquiaValorMonetario", nvl(ass.limite_monetario, 0),
                        "franquiaQuantidade", nvl(ass.limite_quantidade, 0),
                        "limiteAcionamentos", nvl(ass.quantidade_servico, 0),
                        "tipoFranquiaServico", nvl(ass.tipo_limite_franquia, "S"),
                        "prioritario",case  when ass.prioritario = "S"    then true  when ass.prioritario = "N"    then false  else    false end,
                        "ordem", nvl(ass.ordem, 100),
                        "diasAcionamento", nvl(ass.dias_acionamento, 0),
                        "itensNaoInclusos", itn.Descricao.descricaoItem,
                        "questionario", pqrg.Perguntas)
                ) Servicos
                from servico_tb as serv
                left join assistencia_servico_tb as ass    on  ass.servico_id = serv.servico_id and ass.linha = 1
                left join tb_teste_mob2 as itn on itn.servico_id = ass.servico_id and itn.plano_assistencia_id = ass.plano_assistencia_id
                left join assistencia_questionario_tb qst on qst.servico_id = serv.servico_id and qst.linha = 1
                left join tb_teste_mob1 pqrg on qst.questionario_id = pqrg.questionario_id where serv.linha = 1
                 group by ass.plano_assistencia_id;

--tb_teste_mob4
select pl.plano_assistencia_id as planoId,
     nvl(pl.nome_plano, "") as nome,
     cast(nvl(pl.num_contrato, "0") as LONG) as numeroContrato,
     case when plano_referencia = 'S' then true when plano_referencia = 'N' then false else false end planoReferencia,
     smob.Servicos as servicos,
    null categorias
    from plano_assistencia_tb as pl
     left join tb_teste_mob3 as smob on smob.plano_assistencia_id = pl.plano_assistencia_id
      where pl.usuario like "%CARGA_BB_3.0%"  and pl.linha = 1 ;