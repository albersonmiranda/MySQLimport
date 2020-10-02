#' Importar arquivo de impedimentos da GBR
#'
#' Importa o arquivo BC de impedimentos da GBR para o R ou para o MySQL.
#'
#' @export
#'
#' @param caminho String. O caminho do arquivo a ser importado, incluindo a
#'   extensao (.txt).
#' @param MySQL Logical. Se TRUE, entao o arquivo sera importado para o MySQL.
#' @param schema String. Se MySQL = TRUE, o nome do schema que contera a tabela
#'   BC no MySQL. O schema deve existir no MySQL antes de executar a funcao.
#' @param tabela String. Se MySQL = TRUE, o nome da tabela a ser criada no
#'   servidor que contera a os dados importados.
#'
#' @details Se for importar apenas para o R, lembre-se de nomear um objeto para
#'   o resultado da funcao. Caso for importar para o MySQL, nao e necessario
#'   criar um objeto. Note que a tabela será sobrescrita se ja existir.
#'
#' @examples
#' \dontrun{
#' # para importar para o R:
#' data = import_BC("C:\\Arquivos\\GBR\\202009.txt")
#'
#' # para importar para o MySQL:
#' import_BC("C:\\Arquivos\\GBR\\202009.txt", MySQL = TRUE)
#' # ou se nao quiser sobrescrever a tabela:
#' import_BC("C:\\Arquivos\\GBR\\202009.txt", MySQL = TRUE, tabela = "202009")
#' }
#'


import_BC = function (caminho, MySQL = FALSE, schema = "GBR", tabela = "BC") {

  # conexão com MySQL
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
        stop(paste("Talvez você não tenha criado o schema", schema, "no MySQL?"))
      })
  }

  # importar arquivo para o R
  data = readr::read_fwf(
    caminho,
    readr::fwf_widths(
      c(1, 1, 14, 8, 4, 9, 100, 14, 22, 4, 4, 8, 1, 8, 4, 10, 4, 8, 8),
      c("reg", "tipocli", "cpfcnpj", "dt_ini", "cod_adv", "cod_cli", "nome",
        "cpfcnpj2", "num_doc", "produto", "subrproduto", "dt_fim", "ind_resp",
        "dt_proc", "cid", "cod_bureau", "qtd", "dt_pri", "dt_ult")),
    col_types = readr::cols(
      reg = readr::col_character(),
      tipocli = readr::col_character(),
      dt_ini = readr::col_date("%Y%m%d"),
      dt_fim = readr::col_date("%Y%m%d"),
      dt_pri = readr::col_date("%Y%m%d"),
      dt_ult = readr::col_date("%Y%m%d"),
      dt_proc = readr::col_date("%Y%m%d")))

  # removendo header e trailer
  data = data[2:(nrow(data) - 1),]

  # criar tabela no MySQL

  if (MySQL == TRUE) {

    DBI::dbSendQuery(con, paste("DROP TABLE IF EXISTS", tabela))
    DBI::dbWriteTable(con, DBI::SQL(tabela), data)

    # desconectando
    DBI::dbDisconnect(con)
  } else {

    data
  }

}
