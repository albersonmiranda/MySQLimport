#' Importar arquivo de operacoes de credito do SCR/BACEN
#'
#' Importa o arquivo SCR do Banco Central do Brasil com todas as operacoes
#' de instituicoes financeiras dos clientes Banestes para o R ou para o MySQL.
#'
#' @import data.table
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
#' data = import_SCR("C:\\Arquivos\\SCR\\202009.txt")
#'
#' # para importar para o MySQL:
#' import_BC("C:\\Arquivos\\SCR\\202009.txt", MySQL = TRUE, tabela = "t202009")
#' }
#'


import_SCR = function (caminho, MySQL = FALSE, schema = "SCR", tabela) {

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
  xml = xml2::read_xml(caminho)

  unpack_attr = function(xi, i) {
    attrs = attributes(xi)
    if (length(xi) < 1L || is.null(attrs[["names"]]) ) {
      xi = list(. = NA)
    } else {
      xi = list(. = `attributes<-`(xi, NULL))
    }
    c(xi, `[[<-`(attrs, "names", NULL))
  }

  unpack_attrs = function(x, ids = NULL) {
    out = list()
    i = 1L
    while (i <= length(x)) {
      out[[i]] = unpack_attr(x[[i]])
      i = i + 1L
    }
    names(out) = ids
    rbindlist(out, fill = TRUE, idcol = TRUE)
  }

  recur_unpack_attrs = function(xml_tree) {
    out = unpack_attrs(xml_tree)[, .id := as.character(.I)]
    nms = names(out)[-1:-2]
    out_nms = nms
    while (!all(is.na(out[["."]]))) {
      last = copy(out)
      out = out[, unpack_attrs(., .id)]
      tmp = names(out)
      conf = match(out_nms, tmp, 0L); conf = conf[conf > 0L]
      if (length(conf) > 0L) tmp[conf] = paste0(tmp[conf], "_", conf - 2L + length(out_nms))
      out_nms = c(out_nms, tmp[-1:-2])
      names(out) = tmp
      out[last, (nms) := mget(paste0("i.", nms)), on = ".id"][, .id := as.character(.I)]
      nms = names(out)[-1:-2]
    }
    out[rowSums(is.na(out)) < length(out) - 1L, ..out_nms]
  }

  xml_ls = xml2::as_list(xml2::xml_find_all(xml, xpath = "//Cli"))
  index = seq_len(length(xml_ls))
  tasks = split(index, (index - 1L) %/% 50000L)
  data = tibble::as_tibble(rbindlist(lapply(tasks, function(task) recur_unpack_attrs(xml_ls[task])), fill = TRUE))

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
