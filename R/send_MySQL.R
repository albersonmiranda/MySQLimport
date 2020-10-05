#' Envia objeto do R para o MySQL
#'
#' Envia um objeto no ambiente do R para o MySQL. Recomendado quando nao ha
#' memoria suficiente para o envio direto ao MySQL.
#'
#' @export
#'
#' @param objeto O a ser enviado ao MySQL.
#' @param schema String. O nome do schema que contera a tabela.
#' @param tabela String. O nome da tabela a ser criada no
#'   servidor que contera os dados importados.
#'
#' @details Quando um arquivo importado e muito grande, como o SCR, pode nao
#' haver memoria disponivel para enviar o arquivo ao MySQL. Nesse caso, importe
#' ao R, libere memoria e entao envie ao MySQL. Note que a tabela sera
#' sobrescrita se ja existir.
#'
#' @examples
#' \dontrun{
#' # para importar para o R:
#' data = import_SCR("C:\\Arquivos\\SCR\\202009.txt")
#'
#' # para enviar ao MySQL:
#' send_MySQL(data, schema = "SCR", tabela = "t202009")
#' }
#'


send_MySQL = function (objeto, schema, tabela) {


  # conexao com MySQL
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

  # criar tabela no MySQL
    DBI::dbSendQuery(con, paste("DROP TABLE IF EXISTS", tabela))
    DBI::dbWriteTable(con, DBI::SQL(tabela), objeto)

    # desconectando
    DBI::dbDisconnect(con)

}
