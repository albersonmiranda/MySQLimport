#' Importar arquivo de renegociação da gestão de crédito
#'
#' Importa o arquivo GCOA53 para o R ou para o MySQL.
#'
#' @export
#'
#' @param caminho String. O caminho do arquivo a ser importado, incluindo a
#'   extensao (.txt).
#' @param MySQL Logical. Se TRUE, entao o arquivo sera importado para o MySQL.
#' @param schema String. Se MySQL = TRUE, o nome do schema que conterá a tabela
#'   MG5 no MySQL. O schema deve existir no MySQL antes de executar a funcao.
#' @param tabela String. Se MySQL = TRUE, o nome da tabela a ser criada no
#'   servidor que contera os dados importados.
#'
#' @details Se for importar apenas para o R, lembre-se de nomear um objeto para
#'   o resultado da funcao. Caso for importar para o MySQL, nao é necessário
#'   criar um objeto. Note que a tabela será sobrescrita se já existir.
#'
#' @examples
#' \dontrun{
#' # para importar para o R:
#' data = import_MG5("C:\\Arquivos\\MG5\\202009.txt")
#'
#' # para importar para o MySQL:
#' import_MG5("C:\\Arquivos\\MG5\\202009.txt", MySQL = TRUE, tabela = "t202009")
#' }
#'


import_mg5 = function(caminho, MySQL = FALSE, schema = "mg5", tabela) {

  # evaluate args
  if (MySQL == TRUE && is.null(tabela)) {

    stop("tabela deve ser informada")

  }

  # conexao com MySQL
  if (MySQL == TRUE) {

    tryCatch({
      con = DBI::dbConnect(
        RMySQL::MySQL(),
        host = "localhost",
        db = schema,
        user = "root",
        password = rstudioapi::askForPassword("Database password")
      )},
      error = function(cond) {
        message(cond)
        stop(
          paste("Talvez voce nao tenha criado o schema", schema, "no MySQL?")
        )
      })
  }

  # importar arquivo para o R
  data = readr::read_fwf(
    caminho,
    readr::fwf_widths(
      c(4, 7, 8, 4, 4, 20, 4, 4, 20, 17, 1, 12, 2, 1, 12, 12, 12, 9),
      c(
        "anopn", "numeropn", "efetivacao", "produto", "subproduto", "contrato",
        "produtoorigem", "subprodutoorigem", "contratoorigem", "valorpago",
        "situacao", "entrada", "classcontrato", "identificador",
        "valorcontabil", "valorra", "saldocontabil", "matrangariador"
      )
    ),
    col_types = readr::cols(
      anopn = readr::col_character(),
      numeropn = readr::col_character(),
      efetivacao = readr::col_date("%Y%m%d"),
      produto = readr::col_character(),
      subproduto = readr::col_character(),
      contrato = readr::col_character(),
      produtoorigem = readr::col_character(),
      subprodutoorigem = readr::col_character(),
      contratoorigem = readr::col_character(),
      valorpago = readr::col_double(),
      situacao = readr::col_character(),
      entrada = readr::col_double(),
      classcontrato = readr::col_character(),
      identificador = readr::col_character(),
      valorcontabil = readr::col_double(),
      valorra = readr::col_double(),
      saldocontabil = readr::col_double(),
      matrangariador = readr::col_character()
    )
  )

  # criar tabela no MySQL

  if (MySQL == TRUE) {

    DBI::dbSendQuery(con, paste("DROP TABLE IF EXISTS", tabela))

    DBI::dbWriteTable(con, DBI::SQL(tabela), data)

    DBI::dbSendQuery(con, paste("
    ALTER TABLE `", schema, "`.`", tabela, "`
    ADD INDEX idx_efetivacao (efetivacao(8)),
    ADD INDEX idx_produto (produto(4)),
    ADD INDEX idx_subproduto (subproduto(4)),
    ADD INDEX idx_contrato (contrato(20)),
    ADD INDEX idx_produtoorigem (produtoorigem(4)),
    ADD INDEX idx_subprodutoorigem (subprodutoorigem(4)),
    ADD INDEX idx_contratoorigem (contratoorigem(20)),
    ADD INDEX idx_situacao (situacao(1));
    "))

    # desconectando
    DBI::dbDisconnect(con)
  } else {

    data
  }

}
