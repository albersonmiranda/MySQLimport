#' Importar arquivo de unidades do SFB
#'
#' Importa o arquivo GOT78 para o R ou para o MySQL.
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


import_got = function (caminho, MySQL = FALSE, schema = "GOT", tabela) {

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
      c(2, 4, 8, 8, 3, 2, 50, 20 ,30 ,30 ,30 , 30, 2, 5, 3, 4, 8, 8, 8, 8, 8, 4,
        4, 4, 4, 4, 4, 1, 6, 6, 6, 2, 2, 2, 12, 3, 2, 5, 8, 60, 4, 4, 1, 4, 2,
        30, 4, 3, 2, 2, 4, 1, 3, 1, 1, 5, 1, 1, 2, 2, 4, 6, 20, 4, 1, 4, 15, 1),
      c("id_empresa", "cod_dep", "dt_ini", "dt_fim", "tp_dep", "digito",
        "dsc", "dsc_resumida", "logradouro", "compl", "bairro", "municipio",
        "uf", "cep", "compl_cep", "ddd",
        sapply(1:11, function(x) paste0("tab", x)),
        "filler",
        sapply(12:17, function(x) paste0("tab", x)),
        "id_org", "id_subcentro", "rota", "matr_cef", "id_cief", "dsc_pat",
        "dep_contabil", "ag_sub", "id_arq", "id_bacen", "dg_bacen",
        "localizador", "org_sup", "class_ag", "acat_novop", "regiao_fiscal",
        "id_cid_cense", "status_ag_pioneira", "praca_comp", "id_org_adm",
        "id_org_cont", "id_ger_reg", "nivel_aut_dep", "emissao_listao",
        "seq_emissao", "num_vias", "ag_contabil_sub", "cod_local", "dsc_resum",
        "superint", "regiao", "conta_dm_dados", "vlr_acat", "id_tp_dep"
        )
      ),
    col_types = readr::cols(
      cod_dep = readr::col_character(),
      dep_contabil = readr::col_character(),
      superint = readr::col_character(),
      dt_ini = readr::col_date("%Y%m%d"),
      dt_fim = readr::col_date("%Y%m%d")
      )
    )

  # criar tabela no MySQL

  if (MySQL == TRUE) {

    DBI::dbSendQuery(con, paste("DROP TABLE IF EXISTS", tabela))

    DBI::dbWriteTable(con, DBI::SQL(tabela), data)

    DBI::dbSendQuery(con, paste0("
    ALTER TABLE ", schema, ".", tabela,"
    ADD INDEX idx_cod_dep (cod_dep(4)),
    ADD INDEX idx_dep_contabil (dep_contabil(4));
    "))

    # desconectando
    DBI::dbDisconnect(con)
  } else {

    data
  }

}
