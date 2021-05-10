#' Importar arquivo de PNs da gestão de crédito
#'
#' Importa o arquivo C3 para o R ou para o MySQL.
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
#' data <- import_c3
#' 3("C:\\Arquivos\\C3\\202009.txt")
#'
#' # para importar para o MySQL:
#' import_c3("C:\\Arquivos\\C3\\202009.txt", MySQL = TRUE, tabela = "t202009")
#' }
#'
import_c3 <- function(caminho, mysql = FALSE, schema = "c5", tabela) {

    # evaluate args
    if (mysql == TRUE && is.null(tabela)) {
        stop("tabela deve ser informada")
    }

    # conexao com MySQL
    if (mysql == TRUE) {
        tryCatch(
            {
                con <- DBI::dbConnect(
                    RMySQL::MySQL(),
                    host = "localhost",
                    db = schema,
                    user = "root",
                    password = rstudioapi::askForPassword("Database password")
                )
            },
            error = function(cond) {
                message(cond)
                stop(
                    paste(
                        "Talvez voce nao tenha criado o schema",
                        schema,
                        "no MySQL?"
                    )
                )
            }
        )
    }

    # importar arquivo para o R
    data <- readr::read_fwf(
        caminho,
        readr::fwf_widths(
            c(
                4, 1, 14, 100, 4, 7, 1, 15, 2, 2, 2, 4, 4, 2,
                rep(4, 30),
                9,
                rep(c(2, 9, 6, 6, 1), 20),
                rep(c(2, 3, 3), 3),
                8, 1, 4, 1, 4, 15, 3, 1, 15, 1, 15, 9, 1
            ),
            c(
                "agencia", "tp_cli", "cpf_cnpj", "nome", "ano_pn",
                "numero_pn", "status", "valor_pn", "class_pn",
                "class_cliente", "class_garantia", "produto",
                "subproduto", "competencia",
                sapply(1:30, function(x) paste0("exc", x)),
                "proponente",
                unlist(lapply(1:20, function(x) {
                    c(
                        paste0("alcada", x),
                        paste0("mat_alc", x),
                        paste0("data", x),
                        paste0("hora", x),
                        paste0("voto", x)
                    )
                })),
                unlist(lapply(1:3, function(x) {
                    c(
                        paste0("bem", x),
                        paste0("garantia", x),
                        paste0("per_garantia", x)
                    )
                })),
                "taxa", "per_taxa", "prazo", "per_prazo", "indice",
                "desc_indice", "per_indice", "origem_pn", "desc_origem_pn",
                "tp_inclusao_pn", "mat_angariador", "tp_tx_aceita"
            ),
            col_types = readr::cols(
                agencia = readr::col_character(),
                cpf_cnpj = readr::col_character(),
                ano_pn = readr::col_character(),
                numero_pn = readr::col_character(),
                produto = readr::col_character(),
                subproduto = readr::col_character()
            )
        )
    )

    # criar tabela no MySQL

    if (MySQL == TRUE) {
        DBI::dbSendQuery(con, paste("DROP TABLE IF EXISTS", tabela))

        DBI::dbWriteTable(con, DBI::SQL(tabela), data)

        DBI::dbSendQuery(con, paste("
    ALTER TABLE `", schema, "`.`", tabela, "`
    ADD INDEX idx_agencia (agencia(4)),
    ADD INDEX idx_produto (produto(4)),
    ADD INDEX idx_subproduto (subproduto(4)),
    ADD INDEX idx_tp_cli (tp_cli(1));
    "))

        # desconectando
        DBI::dbDisconnect(con)
    } else {
        data
    }
}