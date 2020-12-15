#' Importar arquivo da base geral de clientes do GCB
#'
#' Importa o arquivo BG para o R ou para o MySQL.
#'
#' @export
#'
#' @param caminho String. O caminho do arquivo a ser importado, incluindo a
#'   extensao (.txt).
#' @param tipo String. pf para pessoa fisica e pj para pessoa juridica.
#' @param MySQL Logical. Se TRUE, entao o arquivo sera importado para o MySQL.
#' @param schema String. Se MySQL = TRUE, o nome do schema que contera a tabela
#'   BG no MySQL. O schema deve existir no MySQL antes de executar a funcao.
#' @param tabela String. Se MySQL = TRUE, o nome da tabela a ser criada no
#'   servidor que contera os dados importados.
#'
#' @details Se for importar apenas para o R, lembre-se de nomear um objeto para
#'   o resultado da funcao. Caso for importar para o MySQL, nao e necessario
#'   criar um objeto. Note que a tabela sera sobrescrita se ja existir.
#'
#' @examples
#' \dontrun{
#' # para importar para o R:
#' data = import_BG("C:\\Arquivos\\GCB\\IF041020.txt", "pf")
#'
#' # para importar para o MySQL:
#' import_GCO("C:\\Arquivos\\GCB\\IF041020.txt", "pf", MySQL = TRUE, tabela = "bg")
#' }
#'


import_BG = function (caminho, tipo, MySQL = FALSE, schema = "gcb", tabela) {

  # evaluate args
  if (MySQL == TRUE && is.null(tabela)) {

    stop("tabela deve ser informada")

  }

  # conexao com MySQL
  if (MySQL == TRUE) {

    tryCatch({
      con = DBI::dbConnect(RMySQL::MySQL(),
                           host = "localhost",
                           db = schema,
                           user = "root",
                           password = rstudioapi::askForPassword("Database password")
      )},
      error = function (cond) {
        message(cond)
        stop(paste("Talvez voce nao tenha criado o schema", schema, "no MySQL?"))
      })
  }

  # importar arquivo para o R
  data = readr::read_fwf(
    caminho,
    readr::fwf_widths(
      c(1, 1, 14, 1, 2, 100, 150, 4, 4, 1, 4, 1, 4, 8, 8, 3, 4, 8, 1, 8, 30, 2, 1, 4, 40, 40, 1,
        15, 10, 2, 8, 1, 1, 14, 100, 2, 1, 1, 30, 6, 8, 60, 14, 30, 4, 8, 11, 1, 8, 1, 1, 11,
        1, 8, 30, 6, 30, 20, 8, 30, 2, 4, 8, 4, 10, 40, 1, 1, 1, 12, 8, 6, 2, 10, 2, 2, 10, 2, 2,
        17, 1, 1, 4, 4, 4, 4, 4, 25, 4, 25, 4, 25, 4, 25, 4, 5, 100, 50, 50, 50, 10, 1, 1, 14, 100,
        30, 6, 30, 20, 8, 30, 2, 8),
      col_names = c(
        "tp_reg", "tp_cli", "cpf",
        "titularidade", "class_cliente", "nome",
        "nome_social", "ano_obito", "cid_origem",
        "canal_origem_cadastro", "cid_atual_cadastro", "canal_atual_cadastro",
        "cid_responsavel_doc", "dt_cadastro", "dt_vencimento",
        "cod_banco", "ag_banco", "dt_sis_fin",
        "id_comprovacao_doc", "dt_nascimento", "cidade_nascimento",
        "uf_nascimento", "sexo", "id_nacionalidade",
        "nome_mae", "nome_pai", "tp_documento",
        "documento", "org_emissor", "uf_org_emissor",
        "dt_emissao_doc", "estado_civil", "regime_casamento",
        "cof_conjuge", "nome_conjuge", "filhos",
        "grau_instrucao", "situacao_instrucao", "nome_curso",
        "dt_fim_curso", "dt_inclusao_curso", "empresa_fonte_renda",
        "cnpj_fonte_renda", "cargo", "cbo",
        "dt_admissao", "renda", "tp_renda",
        "dt_renda", "tp_fonte_renda", "tp_vinculo_fonte_renda",
        "renda_total", "tp_renda_total", "dt_renda_total",
        "logradouro", "logradouro_num", "logradouro_complemento",
        "bairro", "cep", "cidade",
        "uf", "ddd", "telefone",
        "ddd_celular", "celular", "email",
        "id_imoveis", "id_veiculos", "id_seguro",
        "patrimonio", "dt_ult_score", "hora_ult_score",
        "modelo", "score", "nivel_risco_calc",
        "class_calc", "score_final", "nivel_risco_final",
        "class_final", "saldo_contabil", "us_person",
        "outra_cidadania",
        "cod_pais_cidadania_1", "cod_pais_cidadania_2",
        "cod_pais_cidadania_3", "cod_pais_cidadania_4",
        "cod_pais_domicilio_1", "nif_1",
        "cod_pais_domicilio_2", "nif_2",
        "cod_pais_domicilio_3", "nif_3",
        "cod_pais_domicilio_4", "nif_4",
        "cod_pais_residencia", "endereco_exterior_numero", "endereco_exterior_logradouro",
        "endereco_exterior_cidade", "endereco_exterior_uf", "endereco_exterior_pais",
        "endereco_exterior_cod_postal",
        "tp_relacionamento", "tp_responsavel", "cpf_relacionamento", "nome_relacionamento",
        "logradouro_digital", "numero_logradouro_digital", "complemento_digital",
        "bairro_digital", "cep_digital", "cidade_digital", "uf_digital", "dt_ultima_alteracao")),
    col_types = readr::cols(
      renda = readr::col_double(),
      renda_total = readr::col_double(),
      patrimonio = readr::col_double(),
      saldo_contabil = readr::col_double(),
      dt_cadastro = readr::col_date("%Y%m%d"),
      dt_vencimento = readr::col_date("%Y%m%d"),
      dt_sis_fin = readr::col_date("%Y%m%d"),
      dt_nascimento = readr::col_date("%Y%m%d"),
      dt_emissao_doc = readr::col_date("%Y%m%d"),
      dt_fim_curso = readr::col_date("%Y%m%d"),
      dt_inclusao_curso = readr::col_date("%Y%m%d"),
      dt_admissao = readr::col_date("%Y%m%d"),
      dt_renda = readr::col_date("%Y%m%d"),
      dt_renda_total = readr::col_date("%Y%m%d"),
      dt_ult_score = readr::col_date("%Y%m%d"),
      dt_ultima_alteracao = readr::col_date("%Y%m%d")),
    skip = 1
  )

  # criar tabela no MySQL

  if (MySQL == TRUE) {

    DBI::dbSendQuery(con, paste("DROP TABLE IF EXISTS", tabela))

    DBI::dbWriteTable(con, DBI::SQL(tabela), data)

    DBI::dbSendQuery(con, paste0("
    ALTER TABLE ", schema, ".", tabela,"
    ADD INDEX idx_cpf (cpf(14));
    "))

    # desconectando
    DBI::dbDisconnect(con)
  } else {

    data
  }

}
