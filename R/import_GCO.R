#' Importar arquivo de contratos de credito do sistema GCO
#'
#' Importa o arquivo GCOA53 para o R ou para o MySQL.
#'
#' @export
#'
#' @param caminho String. O caminho do arquivo a ser importado, incluindo a
#'   extensao (.txt).
#' @param MySQL Logical. Se TRUE, entao o arquivo sera importado para o MySQL.
#' @param schema String. Se MySQL = TRUE, o nome do schema que contera a tabela
#'   GCO no MySQL. O schema deve existir no MySQL antes de executar a funcao.
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
#' data = import_GCO("C:\\Arquivos\\GCO\\202009.txt")
#'
#' # para importar para o MySQL:
#' import_GCO("C:\\Arquivos\\GCO\\202009.txt", MySQL = TRUE, tabela = "t202009")
#' }
#'


import_GCO = function (caminho, MySQL = FALSE, schema = "GCO", tabela) {

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
      c(6, 4, 4, 20, 8, 3, 4, 8, 8, 17, 17, 17, 4, 1, 2, 2, 2, 2, 5, 4, 14,
        30, 13, 1, 1, 2, 8, 20, 2, 1, 1, 13, 11, 2, 2, 2, 2, 17, 9, 1, 4, 1,
        2, 2, 2),
      c("processamento", "produto", "subproduto", "contrato", "clientegco",
        "sitema", "agencia", "efetivacao", "vencimento", "saldodevedor",
        "saldocontabil", "saldopdd", "diasatraso", "adiantamento",
        "classorigem", "class", "classinformada", "classfinal", "grupocontabil",
        "diasprazo", "cpfcnpj", "nome", "vlrutilizadopdd", "prejuizo",
        "tipocredito", "codinterno", "transfprejuizo", "tipoperda", "idempresa",
        "procadm", "procjud", "saldora", "cosif", "classggc",
        "classoperacional", "idpdd", "classrisco", "contratado", "txjuros",
        "tipotx", "indexador", "tipocli", "canalorigem", "canalatual",
        "classoficial")),
    col_types = readr::cols(
      tipocli = readr::col_character(),
      adiantamento = readr::col_character(),
      tipoperda = readr::col_character(),
      procadm = readr::col_character(),
      procjud = readr::col_character(),
      classoperacional = readr::col_character(),
      classoficial = readr::col_character(),
      txjuros = readr::col_double(),
      diasatraso = readr::col_double(),
      diasprazo = readr::col_double(),
      vlrutilizadopdd = readr::col_double(),
      contrato = readr::col_character(),
      saldodevedor = readr::col_double(),
      saldocontabil = readr::col_double(),
      saldora = readr::col_double(),
      contratado = readr::col_double(),
      saldopdd = readr::col_double(),
      processamento = readr::col_character(),
      vencimento = readr::col_date("%Y%m%d"),
      transfprejuizo = readr::col_date("%Y%m%d"),
      efetivacao = readr::col_date("%Y%m%d")))

  # criar tabela no MySQL

  if (MySQL == TRUE) {

    DBI::dbSendQuery(con, paste("DROP TABLE IF EXISTS", tabela))

    DBI::dbWriteTable(con, DBI::SQL(tabela), data)

    DBI::dbSendQuery(con, paste0("
    ALTER TABLE `gco`.`", tabela, "`
    ADD INDEX idx_contrato (contrato(20)),
    ADD INDEX idx_prejuizo (prejuizo(1)),
    ADD INDEX idx_cpfcnpj (cpfcnpj(14)),
    ADD INDEX idx_agencia (agencia(4)),
    ADD INDEX idx_produto (produto(4)),
    ADD INDEX idx_subproduto (subproduto(4)),
    ADD INDEX idx_tipocli (tipocli(1)),
    ADD INDEX idx_procjud (procjud(1)),
    ADD INDEX idx_diasatraso (diasatraso);
    "))

    # desconectando
    DBI::dbDisconnect(con)
  } else {

    data
  }

}
