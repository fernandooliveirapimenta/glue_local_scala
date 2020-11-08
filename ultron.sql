

--tb_teste_mob0
--select rmob.pergunta_id,
--     collect_list(
--             named_struct(
--                    "respostaId", rmob.resposta_id,
--                    "descricaoResposta", nvl(rmob.resposta, ""),
--                    "ordemResposta", nvl(rmob.ordem, 0),
--                    "proximaPerguntaId", rmob.proxima_pergunta,
--                    "razaoResposta", nvl(rmob.razao, "")  )
--     ) as Respostas
--     from assistencia_questionario_resposta_tb rmob
--     where rmob.linha = 1
--      group by rmob.pergunta_id;

--tb_teste_mob1
--select pmob.questionario_id,
--        collect_list(
--                    named_struct(
--                        "perguntaId", pmob.pergunta_id,
--                        "descricaoPergunta", nvl(pmob.pergunta, ""),
--                        "perguntaPreparacao", case  when pmob.preparacao = "S" then true  when pmob.preparacao = "N" then false  else false end,
--                        "multiplasRespostas", case  when pmob.multiplas_respostas = "S" then true  when pmob.multiplas_respostas = "N" then false  else false end,
--                        "razaoPergunta", pmob.razao,
--                        "respostas", rmob.Respostas)
--            ) as Perguntas
--        from assistencia_questionario_pergunta_tb pmob
--         left join tb_teste_mob0 rmob  on pmob.pergunta_id = rmob.pergunta_id
--          where pmob.linha = 1 group by pmob.questionario_id;

--tb_teste_mob2
--select itn.servico_id,
--     itn.plano_assistencia_id,
--     collect_list(
--            named_struct(
--                "descricaoItem", nvl(itn.descricao, "")
--                )
--         ) as Descricao
--     from itens_nao_inclusos_tb as itn
--      where itn.linha = 1
--      group by itn.servico_id, itn.plano_assistencia_id;

--tb_teste_mob0 tipoFranquiaServico fixo M
select ofp.id_oferta_plano,
             collect_list(
                    named_struct(
                        "servicoId", ass.id_assistencia,
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
                        "itensNaoInclusos", null,
                        "questionario", null)
                ) Servicos
                from oferta_plano ofp
                join oferta_plano_pacote_assistencia ofpa on ofpa.id_oferta_plano = ofp.id_oferta_plano
                join oferta_pacote_assistencia opa on opa.id_oferta_pacote_assistencia = ofpa.id_oferta_pacote_assistencia
                join chassi_pacote_assistencia cpa on cpa.id_chassi_pacote_assistencia = opa.id_chassi_pacote_assistencia
                join pacote_assistencia pass on pass.id_pacote_assistencia = cpa.id_pacote_assistencia
                join assistencia_pacote_assistencia apass on apass.id_pacote_assistencia = pass.id_pacote_assistencia
                join assistencia ass on ass.id_assistencia = apass.id_assistencia
                 group by ofp.id_oferta_plano;


--tb_teste_mob1 -- definir plano top referencia
select principal.id_oferta_plano from
    (
        select
            ofp.id_oferta_plano,  count(pass.id_pacote_assistencia) as qtdPacotes
            from oferta_plano ofp
        join oferta_plano_pacote_assistencia ofpa on ofpa.id_oferta_plano = ofp.id_oferta_plano
        join oferta_pacote_assistencia opa on opa.id_oferta_pacote_assistencia = ofpa.id_oferta_pacote_assistencia
        join chassi_pacote_assistencia cpa on cpa.id_chassi_pacote_assistencia = opa.id_chassi_pacote_assistencia
        join pacote_assistencia pass on pass.id_pacote_assistencia = cpa.id_pacote_assistencia
        group by  ofp.id_oferta_plano
    ) principal

    where principal.qtdPacotes = (
            select  max(a.qtdPacotes)
            from (
            select
                ofp.id_oferta_plano,  count(pass.id_pacote_assistencia) as qtdPacotes
                from oferta_plano ofp
            join oferta_plano_pacote_assistencia ofpa on ofpa.id_oferta_plano = ofp.id_oferta_plano
            join oferta_pacote_assistencia opa on opa.id_oferta_pacote_assistencia = ofpa.id_oferta_pacote_assistencia
            join chassi_pacote_assistencia cpa on cpa.id_chassi_pacote_assistencia = opa.id_chassi_pacote_assistencia
            join pacote_assistencia pass on pass.id_pacote_assistencia = cpa.id_pacote_assistencia
            group by  ofp.id_oferta_plano
                ) a
    )

--tb_teste_mob2 final
select pl.id_oferta_plano as planoId,
     nvl(pl.nm_plano, "") as nome,
     cast("0" as LONG) as numeroContrato,
     case when ptop.id_oferta_plano is not null then true else false end planoReferencia,
     smob.Servicos as servicos,
    null categorias
    from oferta_plano as pl
     left join tb_teste_mob1 as ptop on ptop.id_oferta_plano = pl.id_oferta_plano
     left join tb_teste_mob0 as smob on smob.id_oferta_plano = pl.id_oferta_plano


 Guilherme, boa tarde!

 Precisamos conversar com você sobre algumas informações que o nosso modelo de dados do Redis de Assistência possui e que surgiu a dúvida se deve ser preenchido alguma informação padrão ou se pode ficar vazio.

 Observando o exemplo abaixo, temos as seguintes informações a serem definidas:

 numeroContrato - esse número não tem no Ultron, mas é importante para o download dos manuais de assistência.
 Pelo que vi no seu script esse número de contrato é um código AMA que teremos que criar para os produtos do ULTRON, justamente para permitir o Download dos manuais;
 planoReferencia - essa informação diz qual é o plano TOP, haverá a necessidade de definirmos um plano TOP para esse produto do Ultron?
 Não há esse necessidade, acredito que essa informação seja por conta da implantação do TOP 10 dos produtos de Vida, preciso confirmar com o Raphael.
 O plano top é o Plano Total com todos os pacotes de assistência.
 servicoCodigoMapfre - esse número não tem no Ultron.
 É o código do serviço, podemos usar o que está cadastrado no Ultron
 Pode preencher com qualquer valor, visto que não faremos o acionamento nesse momento.
 franquiaValor Monetario
 Temos essa informação na planilha que criei, é o valor monetário de cada serviço vinculado ao plano.
 franquiaQuantidade
 Essa informação são para produtos que não tem um valor monetário, no caso dos serviços do Ultron não teremos essa informação.
 limiteAcionamentos
 É o limite anual de acionamentos, essa informação não exibiremos nesse primeiro momento.
 tipoFranquiaServico
 Preciso entender com o @Raphael Neves.
 Para esse produto são todos monetários (colocar a letra M).
 prioritario - informação que tem a ver com a ordem de exibição no app mobile.
 Temos essa informação na planilha que criei.
 ordem - informação que tem a ver com a ordem de exibição no app mobile.
 Temos essa informação na planilha que criei.
 diasAcionamento
 Não utilizaremos essa informação no momento, pois não teremos a jornada de acionamento do serviço nessa fase.
 Preencher com zero.
 itensNaoInclusos - haverão itens não inclusos a exibir para esse produto do Ultron?
 Temos essa informação na planilha que criei.