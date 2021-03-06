  --"tb_teste_mob0":
   select assistencia_questionario_resposta_tb.pergunta_id,
   collect_list( named_struct(
    "respostaId", resposta_id ,
    "descricaoResposta", nvl(resposta, '') ,
    "proximaPerguntaId", nvl(proxima_pergunta, 0) ,
    "ordemResposta", nvl(ordem, 0) ,
    "razaoResposta", nvl(razao, '') ,
    "tipoResposta", nvl(assistencia_questionario_tp_resposta_tb.nome, '') ,
    "campoReferencia", nvl(campo_referencia, '') ,
    "dadosFonteExterna", case when dados_fonte_externa = 'S' then true when dados_fonte_externa = 'N' then false else false end ) )
     AssistenciaQuestionarioRespostas f
     rom assistencia_questionario_resposta_tb
      left join assistencia_questionario_tp_resposta_tb on assistencia_questionario_resposta_tb.tp_resposta_id = assistencia_questionario_tp_resposta_tb.tp_resposta_id
       and assistencia_questionario_tp_resposta_tb.linha = 1
       where assistencia_questionario_resposta_tb.linha = 1 group by assistencia_questionario_resposta_tb.pergunta_id;

  --"tb_teste_mob1":
   select assistencia_questionario_pergunta_tb.questionario_id,
  collect_list( named_struct( "perguntaId", assistencia_questionario_pergunta_tb.pergunta_id
  ,"descricaoPergunta", nvl(pergunta, '') ,
  "perguntaPreparacao", case when preparacao = 'S' then true when preparacao = 'N' then false else false end ,
  "multiplasRespostas", case when multiplas_respostas = 'S' then true when multiplas_respostas = 'N' then false else false end ,
  "razaoPergunta", nvl(razao, '') ,
  "tipoPergunta", nvl(assistencia_questionario_tp_pergunta_tb.nome, '') ,
  "respostas", AssistenciaQuestionarioRespostas ) ) AssistenciaQuestionarioPerguntas
  from assistencia_questionario_pergunta_tb
  left join assistencia_questionario_tp_pergunta_tb on assistencia_questionario_pergunta_tb.tp_pergunta_id = assistencia_questionario_tp_pergunta_tb.tp_pergunta_id
                and assistencia_questionario_tp_pergunta_tb.linha = 1
  left join tb_teste_mob0 on assistencia_questionario_pergunta_tb.pergunta_id = tb_teste_mob0.pergunta_id
  group by assistencia_questionario_pergunta_tb.questionario_id;

--  "tb_teste_mob2":
 select assistencia_questionario_tb.servico_id,
   tb_teste_mob1.AssistenciaQuestionarioPerguntas as questionarios
   from assistencia_questionario_tb
   left join tb_teste_mob1 on assistencia_questionario_tb.questionario_id = tb_teste_mob1.questionario_id
    where assistencia_questionario_tb.linha = 1;

--  "tb_teste_mob3":
  select servico_tb.servico_id as servicoId,
   named_struct( "servicoCodigoMapfre", nvl(servico_tb.codigo_ama, '') ,
   "nomeServico", nvl(servico_tb.txt_servico, '') ,
   "descricaoServico", nvl(servico_tb.descricao, '') ,
   "questionario", tb_teste_mob2.questionarios ) Servicos
   from servico_tb
   left join tb_teste_mob2 on servico_tb.servico_id = tb_teste_mob2.servico_id
    where servico_tb.linha = 1;

  -- "tb_teste_mob4":
  select itens_nao_inclusos_tb.servico_id,
   itens_nao_inclusos_tb.plano_assistencia_id,
    itens_nao_inclusos_tb.dt_inicio_vigencia,
     collect_list( named_struct( "descricao", descricao ) ) ItensNaoInclusos
     from itens_nao_inclusos_tb
      where itens_nao_inclusos_tb.linha = 1
      group by itens_nao_inclusos_tb.servico_id,
               itens_nao_inclusos_tb.plano_assistencia_id,
               itens_nao_inclusos_tb.dt_inicio_vigencia;

 -- "tb_teste_mob5":
  select assistencia_servico_tb.plano_assistencia_id as planoId,
   assistencia_servico_tb.dt_inicio_vigencia,
    assistencia_servico_tb.categoria_servico_id as categoriaservicoid,
     collect_list(
     named_struct( "servicoId",  assistencia_servico_tb.servico_id ,
     "servicoCodigoMapfre", tb_teste_mob3.Servicos.servicoCodigoMapfre ,
     "nomeServico", tb_teste_mob3.Servicos.nomeServico,
     "descricaoServico", tb_teste_mob3.Servicos.descricaoServico,
     "franquiaValorMonetario", nvl(limite_monetario, 0),
     "franquiaQuantidade", nvl(limite_quantidade, 0),
     "limiteAcionamentos", nvl(quantidade_servico, 0),
     "tipoFranquiaServico", nvl(tipo_limite_franquia, 'S'),
      "prioritario", case when prioritario = 'S' then true when prioritario = 'N' then false else false end,
      "ordem", nvl(ordem, 100),
      "diasAcionamento", nvl(dias_acionamento, 0),
      "itensNaoInclusos", tb_teste_mob4.ItensNaoInclusos.descricao,
      "questionario", tb_teste_mob3.Servicos.questionario)) AssistenciaServicos
       from assistencia_servico_tb
        left join tb_teste_mob3 on assistencia_servico_tb.servico_id = tb_teste_mob3.servicoId
        left join tb_teste_mob4 on assistencia_servico_tb.servico_id =  tb_teste_mob4.servico_id
            and assistencia_servico_tb.plano_assistencia_id = tb_teste_mob4.plano_assistencia_id
            and assistencia_servico_tb.dt_inicio_vigencia = tb_teste_mob4.dt_inicio_vigencia
        where assistencia_servico_tb.linha = 1
        group by assistencia_servico_tb.plano_assistencia_id,
         assistencia_servico_tb.dt_inicio_vigencia,
          assistencia_servico_tb.categoria_servico_id;

  --"tb_teste_mob6":
   select tb_teste_mob5.planoId,
   tb_teste_mob5.dt_inicio_vigencia,
    collect_list(
    named_struct(  "nome", assistencia_categoria_servico_tb.nome  ,
    "servicos", tb_teste_mob5.AssistenciaServicos  )  ) Categorias
    from assistencia_categoria_servico_tb
     left join tb_teste_mob5  on assistencia_categoria_servico_tb.categoria_servico_id = tb_teste_mob5.categoriaservicoid
      where assistencia_categoria_servico_tb.linha = 1
      group by tb_teste_mob5.planoId, tb_teste_mob5.dt_inicio_vigencia;

 -- "tb_teste_mob7":
 select pl.plano_assistencia_id as planoId,
   pl.nome_plano as nome,
   cast(nvl(pl.num_contrato, '0') as LONG) as numeroContrato,
   case when plano_referencia = 'S' then true when plano_referencia = 'N' then false else false end planoReferencia,
    cmob.Categorias as categorias,
    null as servicos
    from plano_assistencia_tb as pl
    inner join tb_teste_mob6 as cmob on cmob.planoId = pl.plano_assistencia_id
             and cmob.dt_inicio_vigencia = pl.dt_inicio_vigencia where pl.usuario like '%BB%' and pl.linha = 1";